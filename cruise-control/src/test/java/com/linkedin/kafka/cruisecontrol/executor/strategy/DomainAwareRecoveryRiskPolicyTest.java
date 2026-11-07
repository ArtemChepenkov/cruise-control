/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor.strategy;

import java.util.Set;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/** Verifies risk levels and actual improvements in final redundancy. */
public class DomainAwareRecoveryRiskPolicyTest {
  private final RecoveryRiskPolicy _policy = new DomainAwareRecoveryRiskPolicy();

  @Test
  public void testSingleCopyIsCriticalAndDefaultsRewardSecondCopy() {
    RecoveryRiskPolicy.Risk risk = _policy.evaluate(context(1, Set.of("a")));
    assertEquals(RecoveryRiskLevel.CRITICAL, risk.level());
    assertEquals(150.0, risk.benefit(), 0.0);
  }

  @Test
  public void testSingleDomainIsCriticalWithSeveralCopies() {
    RecoveryRiskPolicy.Risk risk = _policy.evaluate(context(3, Set.of("a")));
    assertEquals(RecoveryRiskLevel.CRITICAL, risk.level());
    assertEquals(30.0, risk.benefit(), 0.0);
  }

  @Test
  public void testCopiesBelowTargetAreDegraded() {
    RecoveryRiskPolicy.Risk risk = _policy.evaluate(context(2, Set.of("a", "c")));
    assertEquals(RecoveryRiskLevel.DEGRADED, risk.level());
    assertEquals(50.0, risk.benefit(), 0.0);
  }

  @Test
  public void testDomainsBelowTargetAreDegraded() {
    RecoveryRiskPolicy.Risk risk = _policy.evaluate(context(3, Set.of("a", "c")));
    assertEquals(RecoveryRiskLevel.DEGRADED, risk.level());
    assertEquals(30.0, risk.benefit(), 0.0);
  }

  @Test
  public void testSafeRelocationAndCopiesAboveTargetHaveNoBenefit() {
    RecoveryTaskContext context = context(3, Set.of("a", "b", "c"));
    RecoveryRiskPolicy.Risk risk = _policy.evaluate(context, 3, Set.of("a", "b", "c"));
    assertEquals(RecoveryRiskLevel.NORMAL, risk.level());
    assertEquals(0.0, risk.benefit(), 0.0);
    assertEquals(0.0, _policy.evaluate(context, 5, Set.of("a", "b", "c", "d")).benefit(), 0.0);
  }

  @Test
  public void testReplacingExistingDomainDoesNotInventDomainGain() {
    RecoveryRiskPolicy.Risk risk = _policy.evaluate(context(2, Set.of("a", "b")), 2, Set.of("a", "c"));
    assertEquals(0.0, risk.benefit(), 0.0);
  }

  @Test
  public void testUnknownDomainsDoNotInventSingleDomainOrDomainGain() {
    RecoveryRiskPolicy.Risk risk = _policy.evaluate(context(3, Set.of()), 3, Set.of("a", "b", "c"));
    assertEquals(RecoveryRiskLevel.DEGRADED, risk.level());
    assertEquals(0.0, risk.benefit(), 0.0);
    assertEquals(0.0, _policy.evaluate(context(3, Set.of("a", "c")), 3, Set.of()).benefit(), 0.0);
  }

  @Test
  public void testNoSourceCopyHasNoRecoverableBenefit() {
    RecoveryRiskPolicy.Risk risk = _policy.evaluate(context(0, Set.of()), 3, Set.of("a", "b", "c"));
    assertEquals(RecoveryRiskLevel.CRITICAL, risk.level());
    assertEquals(0.0, risk.benefit(), 0.0);
  }

  @Test
  public void testWeightsAreConfigurableAndCountAllRestoredCopiesAndDomains() {
    RecoveryRiskPolicy policy = new DomainAwareRecoveryRiskPolicy(11.0, 2.0, 7.0);
    assertEquals(20.0, policy.evaluate(context(1, Set.of("a"))).benefit(), 0.0);
    assertEquals(29.0, policy.evaluate(context(1, Set.of("a")), 3, Set.of("a", "b", "c")).benefit(), 0.0);
  }

  private static RecoveryTaskContext context(int copies, Set<String> domains) {
    return new RecoveryTaskContext("topic", 0, 3, copies, 3, domains, 1048576, 1.0,
                                   1, 2, "b", null, null, 1000, 1000);
  }
}
