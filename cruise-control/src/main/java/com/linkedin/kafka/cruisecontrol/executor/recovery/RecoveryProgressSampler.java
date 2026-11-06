/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.kafka.cruisecontrol.executor.recovery;

import com.linkedin.kafka.cruisecontrol.executor.ExecutionTask;
import com.linkedin.kafka.cruisecontrol.executor.ExecutionTaskManager;
import com.linkedin.kafka.cruisecontrol.executor.ExecutionTaskState;
import com.linkedin.kafka.cruisecontrol.model.ReplicaPlacementInfo;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.LogDirDescription;
import org.apache.kafka.clients.admin.ReplicaInfo;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.utils.Time;


/** Estimates replica catch-up using log directory sizes for one execution. */
public final class RecoveryProgressSampler {
  private static final long MIB = 1024L * 1024;
  private static final long LOG_DIR_TIMEOUT_SECONDS = 5;

  private final Admin _admin;
  private final ExecutionTaskManager _taskManager;
  private final Time _time;
  private RecoveryProgressSnapshot _previous;

  /**
   * Create a sampler for an execution.
   *
   * @param admin the existing Cruise Control admin client
   * @param taskManager the source of planner estimates when measurements are unavailable
   * @param time the clock for measurement timestamps
   */
  public RecoveryProgressSampler(Admin admin, ExecutionTaskManager taskManager, Time time) {
    _admin = admin;
    _taskManager = taskManager;
    _time = time;
  }

  /**
   * Sample active inter-broker movements and include queued bytes from the task manager.
   *
   * @param tasks the current execution tasks
   * @return a backlog and net progress snapshot
   */
  public synchronized RecoveryProgressSnapshot sample(Collection<ExecutionTask> tasks) {
    List<ExecutionTask> active = tasks.stream()
        .filter(task -> task.type() == ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION
            && task.state() == ExecutionTaskState.IN_PROGRESS)
        .toList();
    long pendingBytes = _taskManager.remainingInterBrokerDataToMoveInMB() * MIB;
    long measured = measuredBytes(active);
    boolean estimated = measured < 0;
    long remaining = pendingBytes + (estimated ? _taskManager.inExecutionInterBrokerDataToMoveInMB() * MIB : measured);
    long now = _time.milliseconds();
    long rate = 0;
    if (_previous != null && now > _previous.timestampMs() && estimated == _previous.estimated()) {
      long decrease = Math.max(0, _previous.remainingBytes() - remaining);
      rate = Math.round(decrease * 1000.0 / (now - _previous.timestampMs()));
    }
    _previous = new RecoveryProgressSnapshot(now, remaining, rate, estimated);
    return _previous;
  }

  private long measuredBytes(List<ExecutionTask> active) {
    if (active.isEmpty()) {
      return 0;
    }
    Set<Integer> brokers = new HashSet<>();
    for (ExecutionTask task : active) {
      brokers.add(task.proposal().oldLeader().brokerId());
      for (ReplicaPlacementInfo replica : task.proposal().replicasToAdd()) {
        brokers.add(replica.brokerId());
      }
    }

    try {
      Map<Integer, KafkaFuture<Map<String, LogDirDescription>>> descriptions = _admin.describeLogDirs(brokers).descriptions();
      KafkaFuture.allOf(descriptions.values().toArray(KafkaFuture<?>[]::new)).get(LOG_DIR_TIMEOUT_SECONDS, TimeUnit.SECONDS);
      long remaining = 0;
      for (ExecutionTask task : active) {
        TopicPartition partition = task.proposal().topicPartition();
        long source = replicaSize(descriptions.get(task.proposal().oldLeader().brokerId()), partition);
        for (ReplicaPlacementInfo replica : task.proposal().replicasToAdd()) {
          long target = replicaSize(descriptions.get(replica.brokerId()), partition);
          if (source < 0 || target < 0) {
            return -1;
          }
          remaining += Math.max(source - target, 0);
        }
      }
      return remaining;
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      return -1;
    } catch (ExecutionException | TimeoutException | KafkaException unavailable) {
      return -1;
    }
  }

  private static long replicaSize(KafkaFuture<Map<String, LogDirDescription>> result, TopicPartition partition)
      throws ExecutionException, InterruptedException {
    if (result == null) {
      return -1;
    }
    for (LogDirDescription directory : result.get().values()) {
      if (directory.error() != null) {
        continue;
      }
      ReplicaInfo replica = directory.replicaInfos().get(partition);
      if (replica != null && !replica.isFuture()) {
        return replica.size();
      }
    }
    return -1;
  }
}
