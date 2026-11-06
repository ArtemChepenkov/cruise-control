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

import com.linkedin.kafka.cruisecontrol.executor.ExecutionProposal;
import com.linkedin.kafka.cruisecontrol.executor.ExecutionTask;
import com.linkedin.kafka.cruisecontrol.executor.ExecutionTaskManager;
import com.linkedin.kafka.cruisecontrol.model.ReplicaPlacementInfo;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeLogDirsResult;
import org.apache.kafka.clients.admin.LogDirDescription;
import org.apache.kafka.clients.admin.ReplicaInfo;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.MockTime;
import org.junit.Test;

import static com.linkedin.kafka.cruisecontrol.executor.recovery.AimdThrottleControlPolicyTest.MIB;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


/** Checks the sampler against broker size responses and the task manager's fallback. */
public class RecoveryProgressSamplerTest {
  private static final TopicPartition PARTITION = new TopicPartition("task11", 0);
  private final MockTime _time = new MockTime(0, 120000, 0);

  @Test
  public void testMeasuresRemainingBytesAndNetProgressAcrossSamples() {
    Admin admin = mock(Admin.class);
    ExecutionTaskManager manager = mock(ExecutionTaskManager.class);
    expect(manager.remainingInterBrokerDataToMoveInMB()).andReturn(20L).times(2);
    expect(admin.describeLogDirs(Set.of(1, 2))).andReturn(logDirs(100 * MIB, 40 * MIB));
    expect(admin.describeLogDirs(Set.of(1, 2))).andReturn(logDirs(102 * MIB, 52 * MIB));
    replay(admin, manager);

    RecoveryProgressSampler sampler = new RecoveryProgressSampler(admin, manager, _time);
    List<ExecutionTask> tasks = List.of(task());
    RecoveryProgressSnapshot first = sampler.sample(tasks);
    assertFalse(first.estimated());
    assertEquals(80 * MIB, first.remainingBytes());
    assertEquals(0, first.progressBytesPerSecond());
    _time.sleep(10000);
    RecoveryProgressSnapshot second = sampler.sample(tasks);
    assertEquals(70 * MIB, second.remainingBytes());
    assertEquals(MIB, second.progressBytesPerSecond());
    verify(admin, manager);
  }

  @Test
  public void testUnavailableReplicaUsesDiscreteEstimateWithoutInventingProgress() {
    Admin admin = mock(Admin.class);
    ExecutionTaskManager manager = mock(ExecutionTaskManager.class);
    expect(manager.remainingInterBrokerDataToMoveInMB()).andReturn(20L).times(2);
    expect(manager.inExecutionInterBrokerDataToMoveInMB()).andReturn(100L);
    expect(admin.describeLogDirs(Set.of(1, 2))).andReturn(logDirs(100 * MIB, -1));
    expect(admin.describeLogDirs(Set.of(1, 2))).andReturn(logDirs(100 * MIB, 50 * MIB));
    replay(admin, manager);

    RecoveryProgressSampler sampler = new RecoveryProgressSampler(admin, manager, _time);
    List<ExecutionTask> tasks = List.of(task());
    RecoveryProgressSnapshot estimated = sampler.sample(tasks);
    assertTrue(estimated.estimated());
    assertEquals(120 * MIB, estimated.remainingBytes());
    _time.sleep(10000);
    RecoveryProgressSnapshot measured = sampler.sample(tasks);
    assertFalse(measured.estimated());
    assertEquals(70 * MIB, measured.remainingBytes());
    assertEquals(0, measured.progressBytesPerSecond());
    verify(admin, manager);
  }

  private ExecutionTask task() {
    ReplicaPlacementInfo leader = new ReplicaPlacementInfo(1);
    ExecutionProposal proposal = new ExecutionProposal(PARTITION, 100, leader, List.of(leader),
                                                        List.of(leader, new ReplicaPlacementInfo(2)));
    ExecutionTask task = new ExecutionTask(1, proposal, ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION, 1000);
    task.inProgress(_time.milliseconds());
    return task;
  }

  private static DescribeLogDirsResult logDirs(long sourceBytes, long targetBytes) {
    DescribeLogDirsResult result = mock(DescribeLogDirsResult.class);
    expect(result.descriptions()).andReturn(Map.of(1, directory(sourceBytes), 2, directory(targetBytes)));
    replay(result);
    return result;
  }

  private static KafkaFuture<Map<String, LogDirDescription>> directory(long bytes) {
    Map<TopicPartition, ReplicaInfo> replicas = bytes < 0 ? Map.of() : Map.of(PARTITION, new ReplicaInfo(bytes, 0, false));
    return KafkaFuture.completedFuture(Map.of("/kafka", new LogDirDescription(null, replicas)));
  }
}
