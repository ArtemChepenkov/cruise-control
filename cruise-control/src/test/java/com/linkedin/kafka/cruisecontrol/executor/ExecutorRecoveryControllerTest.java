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

package com.linkedin.kafka.cruisecontrol.executor;

import com.linkedin.cruisecontrol.monitor.sampling.aggregator.AggregatedMetricValues;
import com.linkedin.cruisecontrol.monitor.sampling.aggregator.Extrapolation;
import com.linkedin.cruisecontrol.monitor.sampling.aggregator.MetricValues;
import com.linkedin.cruisecontrol.monitor.sampling.aggregator.ValuesAndExtrapolations;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.executor.recovery.RecoveryController;
import com.linkedin.kafka.cruisecontrol.model.ReplicaPlacementInfo;
import com.linkedin.kafka.cruisecontrol.monitor.metricdefinition.KafkaMetricDef;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.holder.BrokerEntity;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.AlterConfigsResult;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.utils.MockTime;
import org.junit.Test;

import static com.linkedin.kafka.cruisecontrol.executor.ReplicationThrottleHelper.FOLLOWER_REPLICATION_THROTTLED_RATE_CONFIG;
import static com.linkedin.kafka.cruisecontrol.executor.ReplicationThrottleHelper.LEADER_REPLICATION_THROTTLED_RATE_CONFIG;
import static com.linkedin.kafka.cruisecontrol.executor.ReplicationThrottleHelper.LEADER_REPLICATION_THROTTLED_REPLICAS_CONFIG;
import static org.easymock.EasyMock.*;
import static org.junit.Assert.*;


/** Checks the executor opt-in and the broker changes performed by its throttle helper. */
public class ExecutorRecoveryControllerTest {
  private static final long INITIAL = 10L * 1024 * 1024;
  private static final long UPDATED = 20L * 1024 * 1024;

  @Test
  public void testRecoveryOrderingUsesOnlyObservedCpuRatios() {
    short cpuId = KafkaMetricDef.brokerMetricDef().metricInfo(KafkaMetricDef.CPU_USAGE.name()).id();
    MetricValues measured = new MetricValues(1);
    measured.set(0, 0.75);
    MetricValues invalid = new MetricValues(1);
    invalid.set(0, 1.25);
    AggregatedMetricValues values = new AggregatedMetricValues(Map.of(cpuId, measured));
    Map<BrokerEntity, ValuesAndExtrapolations> metrics = Map.of(
        new BrokerEntity("measured", 1), new ValuesAndExtrapolations(values, Map.of()),
        new BrokerEntity("estimated", 2), new ValuesAndExtrapolations(values, Map.of(0, Extrapolation.AVG_ADJACENT)),
        new BrokerEntity("missing", 3), new ValuesAndExtrapolations(new AggregatedMetricValues(), Map.of()),
        new BrokerEntity("invalid", 4), new ValuesAndExtrapolations(new AggregatedMetricValues(Map.of(cpuId, invalid)), Map.of()));
    assertEquals(Map.of(1, 0.75), Executor.recoveryBrokerCpuRatios(metrics));
  }

  @Test
  public void testBothSwitchesAreRequiredAndRequestSuppliesInitialRate() {
    KafkaCruiseControlConfig disabled = new KafkaCruiseControlConfig(Map.of("bootstrap.servers", "localhost:9092"));
    KafkaCruiseControlConfig enabled = new KafkaCruiseControlConfig(Map.of("bootstrap.servers", "localhost:9092",
        "recovery.control.enabled", true, "recovery.control.slo.url", "http://localhost:8081/api/load/metrics/current"));
    MockTime time = new MockTime();
    assertNull(Executor.createRecoveryController(disabled, time, "run", INITIAL, true));
    assertNull(Executor.createRecoveryController(enabled, time, "run", INITIAL, false));
    RecoveryController controller = Executor.createRecoveryController(enabled, time, "run", INITIAL, true);
    assertTrue(controller.state().enabled());
    assertEquals("run", controller.state().executionId());
    assertEquals(INITIAL, controller.state().currentThrottleBytesPerSecond());
  }

  @Test
  public void testRateUpdateLeavesReplicaListsAndAppliesToNextBatch() throws Exception {
    BrokerConfigs configs = new BrokerConfigs();
    try (ReplicationThrottleHelper helper = new ReplicationThrottleHelper(configs._admin, INITIAL, Set.of(1))) {
      helper.setThrottles(List.of(proposal(0, 2)));
      String replicas = configs.topicReplicas();
      helper.updateThrottleRate(UPDATED);
      assertEquals(List.of(INITIAL, UPDATED), configs._brokerRates.get(2));
      assertEquals(replicas, configs.topicReplicas());
      helper.setThrottles(List.of(proposal(1, 3)));
      assertEquals(List.of(UPDATED), configs._brokerRates.get(3));
    }
    assertTrue(configs._values.values().stream().allMatch(Map::isEmpty));
  }

  @Test
  public void testPartialRateFailureStillClearsEveryTouchedResource() {
    BrokerConfigs configs = new BrokerConfigs();
    configs._failUpdateForBroker = 3;
    assertThrows(ExecutionException.class, () -> {
      try (ReplicationThrottleHelper helper = new ReplicationThrottleHelper(configs._admin, INITIAL, Set.of(1))) {
        helper.setThrottles(List.of(proposal(0, 2), proposal(1, 3)));
        helper.updateThrottleRate(UPDATED);
      }
    });
    assertEquals(List.of(INITIAL, UPDATED), configs._brokerRates.get(2));
    assertEquals(List.of(INITIAL), configs._brokerRates.get(3));
    assertTrue(configs._values.values().stream().allMatch(Map::isEmpty));
  }

  @Test
  public void testEmergencyCleanupPreservesAnIdenticalBrokerRateItDidNotWrite() {
    BrokerConfigs configs = new BrokerConfigs();
    ConfigResource broker = new ConfigResource(ConfigResource.Type.BROKER, "2");
    String initial = Long.toString(INITIAL);
    Map<String, ConfigEntry> existing = Map.of(
        LEADER_REPLICATION_THROTTLED_RATE_CONFIG, new ConfigEntry(LEADER_REPLICATION_THROTTLED_RATE_CONFIG, initial),
        FOLLOWER_REPLICATION_THROTTLED_RATE_CONFIG, new ConfigEntry(FOLLOWER_REPLICATION_THROTTLED_RATE_CONFIG, initial));
    configs._values.put(broker, new HashMap<>(existing));
    assertThrows(IllegalStateException.class, () -> {
      try (ReplicationThrottleHelper helper = new ReplicationThrottleHelper(configs._admin, INITIAL, Set.of(1))) {
        helper.setThrottles(List.of(proposal(0, 2)));
        throw new IllegalStateException("submission failed");
      }
    });
    assertEquals(existing, configs._values.get(broker));
    assertTrue(configs._brokerRates.isEmpty());
  }

  private static ExecutionProposal proposal(int partition, int destination) {
    ReplicaPlacementInfo leader = new ReplicaPlacementInfo(1);
    return new ExecutionProposal(new TopicPartition("recovery", partition), 1, leader, List.of(leader),
                                 List.of(new ReplicaPlacementInfo(destination)));
  }

  private static final class BrokerConfigs {
    private final AdminClient _admin = mock(AdminClient.class);
    private final Map<ConfigResource, Map<String, ConfigEntry>> _values = new HashMap<>();
    private final Map<Integer, List<Long>> _brokerRates = new HashMap<>();
    private int _failUpdateForBroker = -1;

    BrokerConfigs() {
      expect(_admin.describeConfigs(anyObject())).andAnswer(() -> {
        Collection<ConfigResource> resources = getCurrentArgument(0);
        ConfigResource resource = resources.iterator().next();
        DescribeConfigsResult result = mock(DescribeConfigsResult.class);
        Config config = new Config(List.copyOf(_values.getOrDefault(resource, Map.of()).values()));
        expect(result.all()).andReturn(KafkaFuture.completedFuture(Map.of(resource, config)));
        replay(result);
        return result;
      }).anyTimes();
      expect(_admin.incrementalAlterConfigs(anyObject())).andAnswer(() -> {
        Map<ConfigResource, Collection<AlterConfigOp>> updates = getCurrentArgument(0);
        return alter(updates);
      }).anyTimes();
      replay(_admin);
    }

    private AlterConfigsResult alter(Map<ConfigResource, Collection<AlterConfigOp>> updates) {
      KafkaFutureImpl<Void> completion = new KafkaFutureImpl<>();
      for (Map.Entry<ConfigResource, Collection<AlterConfigOp>> update : updates.entrySet()) {
        ConfigResource resource = update.getKey();
        Map<String, ConfigEntry> values = _values.computeIfAbsent(resource, ignored -> new HashMap<>());
        for (AlterConfigOp op : update.getValue()) {
          ConfigEntry entry = op.configEntry();
          if (op.opType() == AlterConfigOp.OpType.DELETE) {
            values.remove(entry.name());
          } else {
            if (entry.name().equals(LEADER_REPLICATION_THROTTLED_RATE_CONFIG)) {
              int broker = Integer.parseInt(resource.name());
              long rate = Long.parseLong(entry.value());
              assertTrue(update.getValue().stream().anyMatch(candidate ->
                  candidate.configEntry().name().equals(FOLLOWER_REPLICATION_THROTTLED_RATE_CONFIG)
                  && candidate.configEntry().value().equals(entry.value())));
              if (broker == _failUpdateForBroker && rate == UPDATED) {
                completion.completeExceptionally(new IllegalStateException("broker rejected update"));
                break;
              }
              _brokerRates.computeIfAbsent(broker, ignored -> new ArrayList<>()).add(rate);
            }
            values.put(entry.name(), entry);
          }
        }
      }
      completion.complete(null);
      AlterConfigsResult result = mock(AlterConfigsResult.class);
      expect(result.all()).andReturn(completion);
      replay(result);
      return result;
    }

    private String topicReplicas() {
      return _values.get(new ConfigResource(ConfigResource.Type.TOPIC, "recovery"))
          .get(LEADER_REPLICATION_THROTTLED_REPLICAS_CONFIG).value();
    }
  }
}
