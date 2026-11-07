/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor.strategy;

import java.util.HashSet;
import java.util.Set;

/** Estimates the redundancy benefit of a proposed final replica placement. */
public interface RecoveryRiskPolicy {
  /** A risk level and a non-negative weighted improvement in redundancy. */
  record Risk(RecoveryRiskLevel level, double benefit) { }

  /**
   * Assess the addition of the context's destination replica.
   * @param context the current partition and destination snapshot
   * @return the current risk level and expected benefit
   */
  default Risk evaluate(RecoveryTaskContext context) {
    Set<String> resultingDomains = new HashSet<>(context.availableDomains());
    if (!resultingDomains.isEmpty() && context.destinationDomain() != null) {
      resultingDomains.add(context.destinationDomain());
    }
    return evaluate(context, context.availableCopies() + 1, resultingDomains);
  }

  /**
   * Assess all additions and removals in a task together.
   * @param context the current partition snapshot
   * @param resultingCopies the expected available copies after completion
   * @param resultingDomains the expected domains after completion, or empty when rack metadata is unavailable
   * @return the current risk level and expected benefit
   */
  Risk evaluate(RecoveryTaskContext context, int resultingCopies, Set<String> resultingDomains);
}
