/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor.strategy;

import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.ExecutorConfig;
import com.linkedin.kafka.cruisecontrol.executor.ExecutionProposal;
import com.linkedin.kafka.cruisecontrol.executor.ExecutionTask;
import com.linkedin.kafka.cruisecontrol.executor.ExecutionTaskState;
import com.linkedin.kafka.cruisecontrol.model.ReplicaPlacementInfo;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.stream.Collectors;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Time;

/** Prioritizes pending recovery by redundancy risk, benefit per copy second, and waiting time. */
public class RiskAwareReplicaMovementStrategy extends AbstractReplicaMovementStrategy implements RecoveryTaskOrderingStrategy, Configurable {
  private static final long MIB = 1024L * 1024;
  private final Time _time;
  private int _requiredCopies = 3;
  private int _requiredDomains = 3;
  private long _bandwidthBytesPerSecond = 20 * MIB;
  private long _agingPeriodMs = 60000;
  private double _agingBonus = 0.1;
  private RecoveryRiskPolicy _riskPolicy = new DomainAwareRecoveryRiskPolicy();

  public RiskAwareReplicaMovementStrategy() {
    this(Time.SYSTEM);
  }

  public RiskAwareReplicaMovementStrategy(Time time) {
    _time = time;
  }

  @Override
  public void configure(Map<String, ?> configs) {
    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(configs);
    _requiredCopies = config.getInt(ExecutorConfig.RECOVERY_RISK_REQUIRED_COPIES_CONFIG);
    _requiredDomains = config.getInt(ExecutorConfig.RECOVERY_RISK_REQUIRED_DOMAINS_CONFIG);
    _bandwidthBytesPerSecond = config.getLong(ExecutorConfig.RECOVERY_RISK_BANDWIDTH_CONFIG);
    _agingPeriodMs = config.getLong(ExecutorConfig.RECOVERY_RISK_AGING_PERIOD_MS_CONFIG);
    _agingBonus = config.getDouble(ExecutorConfig.RECOVERY_RISK_AGING_BONUS_CONFIG);
    _riskPolicy = new DomainAwareRecoveryRiskPolicy(config.getDouble(ExecutorConfig.RECOVERY_RISK_CRITICAL_WEIGHT_CONFIG),
                                                  config.getDouble(ExecutorConfig.RECOVERY_RISK_COPY_WEIGHT_CONFIG),
                                                  config.getDouble(ExecutorConfig.RECOVERY_RISK_DOMAIN_WEIGHT_CONFIG));
  }

  @Override
  public List<ExecutionTask> order(Collection<ExecutionTask> pendingTasks, StrategyOptions options, long nowMs) {
    Comparator<ExecutionTask> comparator = comparator(options, nowMs);
    return pendingTasks.stream().filter(task -> task.state() == ExecutionTaskState.PENDING).sorted(comparator).collect(Collectors.toList());
  }

  @Override
  public Map<Integer, SortedSet<ExecutionTask>> applyStrategy(Set<ExecutionTask> tasks, StrategyOptions options) {
    Set<ExecutionTask> pendingTasks = tasks.stream().filter(task -> task.state() == ExecutionTaskState.PENDING).collect(Collectors.toSet());
    return super.applyStrategy(pendingTasks, options);
  }

  @Override
  public Comparator<ExecutionTask> taskComparator(StrategyOptions options) {
    return comparator(options, _time.milliseconds());
  }

  @Override
  public Comparator<ExecutionTask> taskComparator(Cluster cluster) {
    return taskComparator(new StrategyOptions.Builder(cluster).build());
  }

  private Comparator<ExecutionTask> comparator(StrategyOptions options, long nowMs) {
    Map<ExecutionTask, Priority> priorities = new HashMap<>();
    return Comparator.comparing((ExecutionTask task) -> priorities.computeIfAbsent(task, candidate -> priority(candidate, options, nowMs)).level())
        .thenComparing((task1, task2) -> Double.compare(priorities.get(task2).score(), priorities.get(task1).score()))
        .thenComparing(task -> task.proposal().topic())
        .thenComparingInt(task -> task.proposal().partitionId())
        .thenComparingInt(task -> priorities.get(task).destinationBroker())
        .thenComparingLong(ExecutionTask::executionId);
  }

  private Priority priority(ExecutionTask task, StrategyOptions options, long nowMs) {
    RecoveryTaskContext context = taskContext(task, options, nowMs);
    RecoveryRiskPolicy.Risk risk = risk(task, options, context);
    long createdTimeMs = context.createdTimeMs() == 0 ? nowMs : context.createdTimeMs();
    long waitingMs = Math.max(0, nowMs - createdTimeMs);
    long completedAgingPeriods = waitingMs / _agingPeriodMs;
    double score = risk.benefit() / Math.max(context.estimatedCopySeconds(), 1.0) + completedAgingPeriods * _agingBonus;
    return new Priority(risk.level(), score, context.destinationBroker());
  }

  /**
   * @param task the pending movement
   * @param options current cluster metadata
   * @param nowMs evaluation time
   * @return the current partition risk and the benefit of the complete proposal
   */
  public RecoveryRiskPolicy.Risk risk(ExecutionTask task, StrategyOptions options, long nowMs) {
    return risk(task, options, taskContext(task, options, nowMs));
  }

  private RecoveryRiskPolicy.Risk risk(ExecutionTask task, StrategyOptions options, RecoveryTaskContext context) {
    Set<Integer> available = availableBrokerIds(task, options.cluster());
    Set<Integer> added = task.proposal().replicasToAdd().stream().map(ReplicaPlacementInfo::brokerId).collect(Collectors.toSet());
    Set<Integer> resulting = task.proposal().newReplicas().stream().map(ReplicaPlacementInfo::brokerId)
        .filter(id -> options.cluster().nodeById(id) != null && (available.contains(id) || added.contains(id)))
        .collect(Collectors.toSet());
    return _riskPolicy.evaluate(context, resulting.size(), domains(resulting, options.cluster()));
  }

  /**
   * @param task the pending movement
   * @param options current metadata and optional broker load ratios
   * @param nowMs the fixed evaluation time
   * @return an immutable context; missing load ratios remain null
   */
  public RecoveryTaskContext taskContext(ExecutionTask task, StrategyOptions options, long nowMs) {
    ExecutionProposal proposal = task.proposal();
    Set<Integer> available = availableBrokerIds(task, options.cluster());
    PartitionInfo partition = options.cluster().partition(proposal.topicPartition());
    int source = partition != null && partition.leader() != null ? partition.leader().id() : proposal.oldLeader().brokerId();
    int destination = proposal.replicasToAdd().stream().mapToInt(ReplicaPlacementInfo::brokerId).min().orElse(-1);
    Node destinationNode = options.cluster().nodeById(destination);
    long sizeMiB = Math.max(0, proposal.intraBrokerDataToMoveInMB());
    long sizeBytes = sizeMiB > Long.MAX_VALUE / MIB ? Long.MAX_VALUE : sizeMiB * MIB;
    Double sourceLoad = options.brokerLoadRatios().get(source);
    Double destinationLoad = proposal.replicasToAdd().stream().map(replica -> options.brokerLoadRatios().get(replica.brokerId()))
        .filter(load -> load != null).max(Double::compare).orElse(null);
    double estimatedSeconds = (double) sizeBytes * proposal.replicasToAdd().size() / _bandwidthBytesPerSecond;
    estimatedSeconds /= Math.min(headroom(sourceLoad), headroom(destinationLoad));
    return new RecoveryTaskContext(proposal.topic(), proposal.partitionId(), _requiredCopies, available.size(), _requiredDomains,
        domains(available, options.cluster()), sizeBytes, estimatedSeconds, source, destination,
        destinationNode == null ? null : destinationNode.rack(), sourceLoad, destinationLoad, task.createdTimeMs(), nowMs);
  }

  private static double headroom(Double loadRatio) {
    return loadRatio == null ? 1.0 : Math.max(0.01, 1.0 - loadRatio);
  }

  private static Set<Integer> availableBrokerIds(ExecutionTask task, Cluster cluster) {
    PartitionInfo partition = cluster.partition(task.proposal().topicPartition());
    if (partition == null) {
      return Set.of();
    }
    Set<Integer> replicas = Arrays.stream(partition.replicas()).map(Node::id).collect(Collectors.toSet());
    Set<Integer> offline = Arrays.stream(partition.offlineReplicas()).map(Node::id).collect(Collectors.toSet());
    return Arrays.stream(partition.inSyncReplicas()).map(Node::id)
        .filter(id -> cluster.nodeById(id) != null && replicas.contains(id) && !offline.contains(id)).collect(Collectors.toSet());
  }

  private static Set<String> domains(Set<Integer> brokerIds, Cluster cluster) {
    Set<String> domains = new HashSet<>();
    for (int id : brokerIds) {
      Node broker = cluster.nodeById(id);
      if (broker == null || broker.rack() == null || broker.rack().isBlank()) {
        return Set.of();
      }
      domains.add(broker.rack());
    }
    return domains;
  }

  @Override
  public String name() {
    return RiskAwareReplicaMovementStrategy.class.getSimpleName();
  }

  /** Immutable comparison key for a task evaluated at the captured comparator time. */
  private record Priority(RecoveryRiskLevel level, double score, int destinationBroker) { }
}
