/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor.strategy;

import java.util.Set;

/** An immutable snapshot used to assess one pending recovery task. Empty domains indicate unavailable rack metadata. */
public record RecoveryTaskContext(String topic,
                                  int partition,
                                  int requiredCopies,
                                  int availableCopies,
                                  int requiredDomains,
                                  Set<String> availableDomains,
                                  long replicaSizeBytes,
                                  double estimatedCopySeconds,
                                  int sourceBroker,
                                  int destinationBroker,
                                  String destinationDomain,
                                  Double sourceLoadRatio,
                                  Double destinationLoadRatio,
                                  long createdTimeMs,
                                  long nowMs) {
  public RecoveryTaskContext {
    availableDomains = Set.copyOf(availableDomains);
  }
}
