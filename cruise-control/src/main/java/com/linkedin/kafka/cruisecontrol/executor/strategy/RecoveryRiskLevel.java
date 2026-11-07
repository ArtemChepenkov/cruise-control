/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor.strategy;

/** The current redundancy risk of a partition, in descending priority order. */
public enum RecoveryRiskLevel {
  CRITICAL, DEGRADED, NORMAL
}
