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

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


/** Covers the policy decision table using fixed inputs. */
public class AimdThrottleControlPolicyTest {
  static final long MIB = 1024L * 1024;
  private static final long NOW = 120000;
  private static final long WINDOW = 60000;
  private final AimdThrottleControlPolicy _policy = new AimdThrottleControlPolicy();

  @Test
  public void testHealthyWindowsAndHysteresis() {
    expect(decide(20, 50, 0, 1, 0), ControlAction.HOLD, 20 * MIB, "SLO_HEALTHY");
    expect(decide(20, 50, 0, 2, 0), ControlAction.INCREASE, 25 * MIB, "SLO_HEALTHY");
    expect(decide(20, 90, 0, 2, 0), ControlAction.HOLD, 20 * MIB, "HYSTERESIS");
  }

  @Test
  public void testSloViolationsAndCooldown() {
    expect(decide(20, 110, 0, 0, 0), ControlAction.DECREASE, 14 * MIB, "SLO_P99_EXCEEDED");
    expect(decide(20, 50, 0.03, 0, 0), ControlAction.DECREASE, 14 * MIB, "SLO_ERRORS_EXCEEDED");
    expect(decide(20, Double.NaN, 1, 0, 0), ControlAction.DECREASE, 14 * MIB, "SLO_ERRORS_EXCEEDED");
    ControlInput cooling = input(20, 50, 0, 250, true, 2, 0, ConstraintState.SATISFIED, NOW - 1000);
    expect(_policy.decide(cooling), ControlAction.HOLD, 20 * MIB, "COOLDOWN");
  }

  @Test
  public void testAbsoluteBoundsAndMaximumStep() {
    expect(decide(98, 50, 0, 2, 0), ControlAction.INCREASE, 100 * MIB, "SLO_HEALTHY");
    expect(decide(1, 110, 0, 0, 0), ControlAction.HOLD, MIB, "SLO_P99_EXCEEDED");
    expect(decide(100, 110, 0, 0, 0), ControlAction.DECREASE, 80 * MIB, "SLO_P99_EXCEEDED");
    expect(decide(7, 110, 0, 0, 0), ControlAction.DECREASE, Math.round(7 * MIB * 0.7), "SLO_P99_EXCEEDED");
  }

  @Test
  public void testStaleInsufficientAndRecoveredMetrics() {
    ControlDecision stale = _policy.decide(input(20, 50, 0, 250, false, 2, 0, ConstraintState.SATISFIED, -1));
    expect(stale, ControlAction.DECREASE, 10 * MIB, "METRICS_STALE");
    assertEquals(ConstraintState.METRICS_STALE, stale.constraintState());

    ControlDecision sparse = _policy.decide(input(20, 50, 0, 50, true, 2, 0, ConstraintState.SATISFIED, -1));
    expect(sparse, ControlAction.HOLD, 20 * MIB, "INSUFFICIENT_OBSERVATIONS");
    assertTrue(sparse.warnings().contains("METRICS_STALE"));

    ControlDecision resumed = _policy.decide(input(20, 50, 0, 250, true, 2, 0, ConstraintState.METRICS_STALE, -1));
    expect(resumed, ControlAction.HOLD, 20 * MIB, "METRICS_RECOVERED_WAIT");
  }

  @Test
  public void testProgressFloorAndConflictingConstraints() {
    expect(decide(2, 50, 0, 0, 3), ControlAction.PROGRESS_GUARD, 5 * MIB, "NO_PROGRESS");
    ControlDecision conflict = decide(5, 110, 0, 0, 3);
    expect(conflict, ControlAction.HOLD, 5 * MIB, "UNSATISFIABLE");
    assertEquals(ConstraintState.CONSTRAINTS_UNSATISFIABLE, conflict.constraintState());
  }

  static RecoveryControlSettings settings() {
    return new RecoveryControlSettings(100, 0.02, 20 * MIB, MIB, 100 * MIB, 10 * MIB, 5 * MIB, 20 * MIB,
                                       0.7, 0.85, 2, 100, 20000, 10000, WINDOW, 30000, 5 * MIB, MIB, 3);
  }

  private ControlDecision decide(long throttle, double p99, double errors, int healthy, int stalled) {
    return _policy.decide(input(throttle, p99, errors, 250, true, healthy, stalled, ConstraintState.SATISFIED, -1));
  }

  private static ControlInput input(long throttle, double p99, double errors, long count, boolean fresh,
                                    int healthy, int stalled, ConstraintState previous, long lastChange) {
    RecoveryControlSettings settings = settings();
    long generatedAt = fresh ? NOW : NOW - 31000;
    Double latency = Double.isNaN(p99) ? null : p99;
    SloMetrics metrics = new SloMetrics(generatedAt - WINDOW, generatedAt, generatedAt, count, latency, latency, latency,
                                        Math.round(count * errors), errors, 100, 100, 0, 0, settings, fresh);
    return new ControlInput(NOW, WINDOW, settings, metrics, fresh, throttle * MIB, 100 * MIB, 0,
                            null, null, healthy, stalled, ControlAction.HOLD, previous, lastChange);
  }

  private static void expect(ControlDecision decision, ControlAction action, long throttle, String reason) {
    assertEquals(action, decision.action());
    assertEquals(throttle, decision.throttleBytesPerSecond());
    assertEquals(reason, decision.reason());
  }
}
