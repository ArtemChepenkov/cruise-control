/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor.strategy;

import com.linkedin.kafka.cruisecontrol.config.constants.ExecutorConfig;
import com.linkedin.kafka.cruisecontrol.executor.ExecutionProposal;
import com.linkedin.kafka.cruisecontrol.executor.ExecutionTask;
import com.linkedin.kafka.cruisecontrol.executor.ExecutionTaskState;
import com.linkedin.kafka.cruisecontrol.model.ReplicaPlacementInfo;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.stream.Collectors;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.MockTime;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/** Verifies fixed-snapshot ordering, final-placement benefit, and compatibility with the upstream scheduler. */
public class RiskAwareReplicaMovementStrategyTest {
  private static final long NOW = 120001;
  private static final long MIB = 1024L * 1024;
  private static final Node[] NODES = {new Node(1, "one", 9092, "a"), new Node(2, "two", 9092, "b"),
                                     new Node(3, "three", 9092, "c"), new Node(4, "four", 9092, "a"),
                                     new Node(5, "five", 9092, "b"), new Node(6, "six", 9092, "c")};
  private final RiskAwareReplicaMovementStrategy _strategy = new RiskAwareReplicaMovementStrategy();

  @Test
  public void testCriticalThenDomainRecoveryThenSafeMovementWithRfFour() {
    ExecutionTask critical = task(1, "critical", 0, 10, ids(1, 6), ids(1, 3), NOW);
    ExecutionTask domain = task(2, "domain", 0, 100, ids(1, 4, 2, 6), ids(1, 4, 2, 3), NOW);
    ExecutionTask safe = task(3, "normal", 0, 1000, ids(1, 2, 3, 6), ids(1, 2, 3, 5), NOW);
    StrategyOptions options = options(List.of(NODES).subList(0, 5),
        partition(critical, ids(1)), partition(domain, ids(1, 4, 2)), partition(safe, ids(1, 2, 3)));
    assertEquals(List.of(critical, domain, safe), _strategy.order(List.of(safe, domain, critical), options, NOW));
    assertEquals(RecoveryRiskLevel.CRITICAL, _strategy.risk(critical, options, NOW).level());
    assertEquals(30.0, _strategy.risk(domain, options, NOW).benefit(), 0.0);
    assertEquals(0.0, _strategy.risk(safe, options, NOW).benefit(), 0.0);
  }

  @Test
  public void testRfThreeOnePerDomainControlHasNoInventedDomainBenefit() {
    ExecutionTask safe = task(1, "rf3", 0, 100, ids(4, 2, 3), ids(1, 2, 3), NOW);
    StrategyOptions options = options(List.of(NODES), partition(safe, ids(4, 2, 3)));
    assertEquals(RecoveryRiskLevel.NORMAL, _strategy.risk(safe, options, NOW).level());
    assertEquals(0.0, _strategy.risk(safe, options, NOW).benefit(), 0.0);
  }

  @Test
  public void testBenefitPerCopySecondAndWaitingBonusWithinRiskLevel() {
    ExecutionTask small = task(1, "small", 0, 20, ids(1, 6), ids(1, 3), NOW);
    ExecutionTask large = task(2, "large", 0, 200, ids(1, 6), ids(1, 3), NOW);
    StrategyOptions options = options(List.of(NODES).subList(0, 5), partition(small, ids(1)), partition(large, ids(1)));
    assertEquals(List.of(small, large), _strategy.order(List.of(large, small), options, NOW));

    ExecutionTask old = task(3, "z-old", 0, 200, ids(1, 2, 3), ids(4, 2, 3), NOW - 60000);
    ExecutionTask fresh = task(4, "a-fresh", 0, 20, ids(1, 2, 3), ids(4, 2, 3), NOW);
    StrategyOptions safeOptions = options(List.of(NODES), partition(old, ids(1, 2, 3)), partition(fresh, ids(1, 2, 3)));
    assertEquals(List.of(fresh, old), _strategy.order(List.of(old, fresh), safeOptions, NOW - 1));
    assertEquals(List.of(old, fresh), _strategy.order(List.of(fresh, old), safeOptions, NOW));
  }

  @Test
  public void testLegacyZeroTimestampDoesNotGainArtificialWaitingPriority() {
    ExecutionTask legacy = task(1, "z-legacy", 0, 20, ids(1, 2, 3), ids(4, 2, 3), 0);
    ExecutionTask fresh = task(2, "a-fresh", 0, 20, ids(1, 2, 3), ids(4, 2, 3), NOW);
    StrategyOptions options = options(List.of(NODES), partition(legacy, ids(1, 2, 3)), partition(fresh, ids(1, 2, 3)));
    assertEquals(List.of(fresh, legacy), _strategy.order(List.of(legacy, fresh), options, NOW));
  }

  @Test
  public void testComparatorCapturesOneTimeForItsLifetime() {
    MockTime time = new MockTime(0, 60000, 0);
    RiskAwareReplicaMovementStrategy strategy = new RiskAwareReplicaMovementStrategy(time);
    ExecutionTask old = task(1, "z-old", 0, 20, ids(1, 2, 3), ids(4, 2, 3), 1);
    ExecutionTask fresh = task(2, "a-fresh", 0, 20, ids(1, 2, 3), ids(4, 2, 3), 60000);
    StrategyOptions options = options(List.of(NODES), partition(old, ids(1, 2, 3)), partition(fresh, ids(1, 2, 3)));
    Comparator<ExecutionTask> frozen = strategy.taskComparator(options);
    assertTrue(frozen.compare(old, fresh) > 0);
    time.sleep(1);
    assertTrue(frozen.compare(old, fresh) > 0);
    assertTrue(strategy.taskComparator(options).compare(old, fresh) < 0);
  }

  @Test
  public void testTiesUseTopicPartitionDestinationAndBaseTaskId() {
    ExecutionTask topic = task(9, "a", 9, 20, ids(1, 2, 3), ids(4, 2, 3), NOW);
    ExecutionTask partition = task(8, "b", 0, 20, ids(1, 2, 3), ids(4, 2, 3), NOW);
    ExecutionTask destination = task(7, "b", 1, 20, ids(1, 2, 3), ids(4, 2, 3), NOW);
    ExecutionTask otherDestination = task(6, "b", 1, 20, ids(1, 2, 3), ids(1, 5, 3), NOW);
    ExecutionTask sameDestination = task(10, "b", 1, 20, ids(1, 2, 3), ids(4, 2, 3), NOW);
    StrategyOptions options = options(List.of(NODES), partition(topic, ids(1, 2, 3)), partition(partition, ids(1, 2, 3)),
                                      partition(destination, ids(1, 2, 3)));
    assertEquals(List.of(topic, partition, destination, sameDestination, otherDestination),
                 _strategy.order(List.of(otherDestination, sameDestination, destination, partition, topic), options, NOW));
    Map<Integer, SortedSet<ExecutionTask>> plan = _strategy.chainBaseReplicaMovementStrategyIfAbsent()
        .applyStrategy(Set.of(destination, sameDestination), options);
    assertEquals(List.of(destination, sameDestination), List.copyOf(plan.get(4)));
  }

  @Test
  public void testActiveTasksAreExcludedWithoutChangingTheirState() {
    ExecutionTask active = task(1, "active", 0, 20, ids(1, 6), ids(1, 3), NOW);
    active.inProgress(NOW);
    ExecutionTask pending = task(2, "pending", 0, 100, ids(1, 2, 3), ids(4, 2, 3), NOW);
    StrategyOptions options = options(List.of(NODES), partition(active, ids(1)), partition(pending, ids(1, 2, 3)));
    assertEquals(List.of(pending), _strategy.order(List.of(active, pending), options, NOW));
    assertTrue(_strategy.applyStrategy(Set.of(active, pending), options).values().stream().noneMatch(tasks -> tasks.contains(active)));
    assertEquals(ExecutionTaskState.IN_PROGRESS, active.state());
    assertEquals(NOW, active.startTimeMs());
  }

  @Test
  public void testAllDestinationsAndFinalRemovalsContributeToBenefit() {
    ExecutionTask multiple = task(1, "multiple", 0, 20, ids(1, 6), ids(1, 2, 3), NOW);
    ExecutionTask replacement = task(2, "replacement", 0, 20, ids(1, 2, 6), ids(1, 3, 6), NOW);
    StrategyOptions options = options(List.of(NODES).subList(0, 5), partition(multiple, ids(1)), partition(replacement, ids(1, 2)));
    assertEquals(200.0, _strategy.risk(multiple, options, NOW).benefit(), 0.0);
    assertEquals(2.0, _strategy.taskContext(multiple, options, NOW).estimatedCopySeconds(), 0.0);
    assertEquals(0.0, _strategy.risk(replacement, options, NOW).benefit(), 0.0);
    Map<Integer, SortedSet<ExecutionTask>> plan = _strategy.applyStrategy(Set.of(multiple), options);
    assertEquals(Set.of(1, 2, 3), plan.keySet());
  }

  @Test
  public void testOnlyLiveAssignedInSyncCopiesCountAndMissingRacksStayUnknown() {
    ExecutionTask task = task(1, "missing", 0, 20, ids(1, 2, 3), ids(1, 2, 4), NOW);
    PartitionInfo inconsistent = new PartitionInfo("missing", 0, node(1), nodes(ids(1, 2, 3)), nodes(ids(1, 2, 3, 6)), nodes(ids(3)));
    StrategyOptions options = options(List.of(NODES), inconsistent);
    assertEquals(2, _strategy.taskContext(task, options, NOW).availableCopies());
    assertEquals(Set.of("a", "b"), _strategy.taskContext(task, options, NOW).availableDomains());

    StrategyOptions missingRack = options(List.of(node(1), new Node(2, "unknown", 9092), node(3), node(4)), partition(task, ids(1, 2)));
    assertEquals(Set.of(), _strategy.taskContext(task, missingRack, NOW).availableDomains());
    assertEquals(20.0, _strategy.risk(task, missingRack, NOW).benefit(), 0.0);
    assertEquals(RecoveryRiskLevel.DEGRADED, _strategy.risk(task, missingRack, NOW).level());
    assertEquals(0.0, _strategy.risk(task, options(List.of(NODES)), NOW).benefit(), 0.0);
  }

  @Test
  public void testOptionalBrokerLoadRatiosAdjustDurationAndRemainImmutable() {
    ExecutionTask task = task(1, "loads", 0, 20, ids(1, 2, 3), ids(4, 2, 3), NOW);
    StrategyOptions unloaded = options(List.of(NODES), partition(task, ids(1, 2, 3)));
    assertEquals(1.0, _strategy.taskContext(task, unloaded, NOW).estimatedCopySeconds(), 0.0);
    assertNull(_strategy.taskContext(task, unloaded, NOW).sourceLoadRatio());
    assertNull(_strategy.taskContext(task, unloaded, NOW).destinationLoadRatio());
    Map<Integer, Double> loads = new HashMap<>(Map.of(1, 0.5, 4, 0.75));
    StrategyOptions loaded = new StrategyOptions.Builder(unloaded.cluster()).brokerLoadRatios(loads).build();
    loads.put(4, 0.0);
    RecoveryTaskContext context = _strategy.taskContext(task, loaded, NOW);
    assertEquals(4.0, context.estimatedCopySeconds(), 0.0);
    assertEquals(Double.valueOf(0.5), context.sourceLoadRatio());
    assertEquals(Double.valueOf(0.75), context.destinationLoadRatio());
  }

  @Test
  public void testConfiguredBandwidthAndWeightsAreUsed() {
    _strategy.configure(Map.of("bootstrap.servers", "localhost:9092",
                              ExecutorConfig.RECOVERY_RISK_BANDWIDTH_CONFIG, 40 * MIB,
                              ExecutorConfig.RECOVERY_RISK_CRITICAL_WEIGHT_CONFIG, 11.0,
                              ExecutorConfig.RECOVERY_RISK_COPY_WEIGHT_CONFIG, 2.0,
                              ExecutorConfig.RECOVERY_RISK_DOMAIN_WEIGHT_CONFIG, 7.0));
    ExecutionTask task = task(1, "configured", 0, 20, ids(1, 6), ids(1, 3), NOW);
    StrategyOptions options = options(List.of(NODES).subList(0, 5), partition(task, ids(1)));
    assertEquals(0.5, _strategy.taskContext(task, options, NOW).estimatedCopySeconds(), 0.0);
    assertEquals(20.0, _strategy.risk(task, options, NOW).benefit(), 0.0);
  }

  private static ExecutionTask task(long id, String topic, int partition, long sizeMiB, int[] oldIds, int[] newIds, long createdTimeMs) {
    ExecutionProposal proposal = new ExecutionProposal(new TopicPartition(topic, partition), sizeMiB, new ReplicaPlacementInfo(oldIds[0]),
        Arrays.stream(oldIds).mapToObj(ReplicaPlacementInfo::new).collect(Collectors.toList()),
        Arrays.stream(newIds).mapToObj(ReplicaPlacementInfo::new).collect(Collectors.toList()));
    return new ExecutionTask(id, proposal, ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION, 10000, createdTimeMs);
  }

  private static PartitionInfo partition(ExecutionTask task, int[] inSync) {
    ExecutionProposal proposal = task.proposal();
    Node[] replicas = proposal.oldReplicas().stream().map(replica -> node(replica.brokerId())).toArray(Node[]::new);
    return new PartitionInfo(proposal.topic(), proposal.partitionId(), node(proposal.oldLeader().brokerId()), replicas, nodes(inSync));
  }

  private static StrategyOptions options(List<Node> brokers, PartitionInfo... partitions) {
    return new StrategyOptions.Builder(new Cluster("test", brokers, List.of(partitions), Set.of(), Set.of())).build();
  }

  private static Node node(int id) {
    return NODES[id - 1];
  }

  private static Node[] nodes(int[] ids) {
    return Arrays.stream(ids).mapToObj(RiskAwareReplicaMovementStrategyTest::node).toArray(Node[]::new);
  }

  private static int[] ids(int... ids) {
    return ids;
  }
}
