/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor.strategy;

import java.util.Set;

/** Scores recovered copies and failure domains against configured safety targets. */
public class DomainAwareRecoveryRiskPolicy implements RecoveryRiskPolicy {
  private final double _criticalWeight;
  private final double _copyWeight;
  private final double _domainWeight;

  public DomainAwareRecoveryRiskPolicy() {
    this(100.0, 20.0, 30.0);
  }

  public DomainAwareRecoveryRiskPolicy(double criticalWeight, double copyWeight, double domainWeight) {
    if (!Double.isFinite(criticalWeight) || criticalWeight < 0
        || !Double.isFinite(copyWeight) || copyWeight < 0
        || !Double.isFinite(domainWeight) || domainWeight < 0) {
      throw new IllegalArgumentException("Recovery risk weights must be finite and non-negative");
    }
    _criticalWeight = criticalWeight;
    _copyWeight = copyWeight;
    _domainWeight = domainWeight;
  }

  @Override
  public Risk evaluate(RecoveryTaskContext context, int resultingCopies, Set<String> resultingDomains) {
    RecoveryRiskLevel level = RecoveryRiskLevel.NORMAL;
    if (context.availableCopies() <= 1 || context.availableDomains().size() == 1) {
      level = RecoveryRiskLevel.CRITICAL;
    } else if (context.availableCopies() < context.requiredCopies() || context.availableDomains().size() < context.requiredDomains()) {
      level = RecoveryRiskLevel.DEGRADED;
    }
    if (context.availableCopies() == 0) {
      return new Risk(level, 0.0);
    }
    double benefit = context.availableCopies() == 1 && resultingCopies >= 2 ? _criticalWeight : 0.0;
    benefit += _copyWeight * Math.max(0, Math.min(context.requiredCopies(), resultingCopies)
                                          - Math.min(context.requiredCopies(), context.availableCopies()));
    if (!context.availableDomains().isEmpty() && !resultingDomains.isEmpty()) {
      benefit += _domainWeight * Math.max(0, Math.min(context.requiredDomains(), resultingDomains.size())
                                              - Math.min(context.requiredDomains(), context.availableDomains().size()));
    }
    return new Risk(level, benefit);
  }
}
