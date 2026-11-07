/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor.strategy;

import com.linkedin.kafka.cruisecontrol.executor.ExecutionTask;
import java.util.Collection;
import java.util.List;

/** Orders pending tasks using one metadata and clock snapshot. */
public interface RecoveryTaskOrderingStrategy {
  /**
   * @param pendingTasks candidate tasks; non-pending tasks are excluded and retain their state
   * @param options current metadata and optional load estimates
   * @param nowMs the fixed evaluation time
   * @return the ordered pending tasks
   */
  List<ExecutionTask> order(Collection<ExecutionTask> pendingTasks, StrategyOptions options, long nowMs);
}
