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

import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.common.utils.MockTime;
import org.junit.Test;

import static com.linkedin.kafka.cruisecontrol.executor.recovery.AimdThrottleControlPolicyTest.MIB;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;


/** Verifies decisions and published state across execution progress checks. */
public class RecoveryControllerTest {
  private final MockTime _time = new MockTime(0, 120000, 0);
  private final RecoveryControlSettings _settings = AimdThrottleControlPolicyTest.settings();
  private final MetricsSource _source = new MetricsSource();

  @Test
  public void testIntervalAndRepeatedWindowsDoNotDoubleCount() {
    RecoveryController controller = controller(new AimdThrottleControlPolicy());
    assertFalse(check(controller).isPresent());
    assertEquals(1, _source._calls);

    _time.sleep(5000);
    assertFalse(check(controller).isPresent());
    assertEquals(1, _source._calls);

    _time.sleep(5000);
    assertFalse(check(controller).isPresent());
    assertEquals(2, _source._calls);

    _time.sleep(10000);
    _source.at(_time.milliseconds());
    ControlDecision changed = check(controller).orElseThrow();
    assertEquals(ControlAction.INCREASE, changed.action());
    assertEquals(25 * MIB, controller.state().currentThrottleBytesPerSecond());
    assertEquals(changed, controller.state().lastDecision());

    _time.sleep(10000);
    _source.at(_time.milliseconds());
    assertFalse(check(controller).isPresent());
    assertEquals("COOLDOWN", controller.state().lastDecision().reason());

    _time.sleep(10000);
    _source.at(_time.milliseconds());
    assertEquals(30 * MIB, check(controller).orElseThrow().throttleBytesPerSecond());

    controller.stop();
    int calls = _source._calls;
    _time.sleep(10000);
    assertFalse(check(controller).isPresent());
    assertFalse(controller.state().enabled());
    assertEquals(calls, _source._calls);
  }

  @Test
  public void testRecoveryWaitsOneFullWindowBeforeResumingPolicy() {
    RecoveryController controller = controller(new AimdThrottleControlPolicy());
    _source._snapshot = new SloMetrics(0, 0, 0, 0, null, null, null, 0, Double.NaN, 0, 0, 0, 0, null, false);
    ControlDecision fallback = check(controller).orElseThrow();
    assertEquals(10 * MIB, fallback.throttleBytesPerSecond());
    assertNull(fallback.evaluatedInput().slo());
    assertEquals(ConstraintState.METRICS_STALE, controller.state().lastDecision().constraintState());

    for (int cycle = 0; cycle <= 6; cycle++) {
      _time.sleep(10000);
      _source.at(_time.milliseconds());
      assertFalse(check(controller).isPresent());
      assertEquals("METRICS_RECOVERED_WAIT", controller.state().lastDecision().reason());
    }
    assertEquals(ConstraintState.SATISFIED, controller.state().lastDecision().constraintState());
    _time.sleep(10000);
    _source.at(_time.milliseconds());
    assertFalse(check(controller).isPresent());
    _time.sleep(10000);
    _source.at(_time.milliseconds());
    assertEquals(15 * MIB, check(controller).orElseThrow().throttleBytesPerSecond());
  }

  @Test
  public void testEstimatedProgressDoesNotTriggerStallDetection() {
    RecoveryController controller = controller(new AimdThrottleControlPolicy());
    for (int cycle = 0; cycle < 4; cycle++) {
      _source.at(_time.milliseconds());
      controller.onProgressCheck(new RecoveryProgressSnapshot(_time.milliseconds(), 100 * MIB, 0, true), null, null);
      assertEquals(0, controller.state().lastDecision().evaluatedInput().consecutiveNoProgressWindows());
      assertTrue(controller.state().warnings().contains("PROGRESS_ESTIMATED"));
      _time.sleep(10000);
    }
    for (int cycle = 0; cycle <= 3; cycle++) {
      _source.at(_time.milliseconds());
      controller.onProgressCheck(new RecoveryProgressSnapshot(_time.milliseconds(), 100 * MIB, 0, false), null, null);
      assertEquals(cycle, controller.state().lastDecision().evaluatedInput().consecutiveNoProgressWindows());
      _time.sleep(10000);
    }
  }

  @Test
  public void testPolicyFailurePublishesSafeThrottleAndExceptionClass() {
    RecoveryController controller = controller(new ThrottleControlPolicy() {
      @Override
      public void configure(Map<String, ?> configs) {
        // The test policy always fails during evaluation.
      }

      @Override
      public ControlDecision decide(ControlInput input) {
        throw new IllegalStateException("private error detail");
      }
    });
    ControlDecision decision = check(controller).orElseThrow();

    assertEquals(10 * MIB, decision.throttleBytesPerSecond());
    assertEquals(ConstraintState.SLO_AT_RISK, decision.constraintState());
    assertEquals("POLICY_FAILURE:IllegalStateException", decision.reason());
    assertEquals(List.of("POLICY_FAILURE"), controller.state().warnings());
    assertEquals(decision, controller.state().lastDecision());
  }

  private RecoveryController controller(ThrottleControlPolicy policy) {
    _source.at(_time.milliseconds());
    return new RecoveryController("task11-test", _time, _settings, _source, policy);
  }

  private Optional<ControlDecision> check(RecoveryController controller) {
    return controller.onProgressCheck(new RecoveryProgressSnapshot(_time.milliseconds(), 100 * MIB, 2 * MIB, false), null, null);
  }

  private final class MetricsSource implements SloMetricsProvider {
    private SloMetrics _snapshot;
    private int _calls;

    @Override
    public void configure(Map<String, ?> configs) {
      // The tests supply snapshots directly.
    }

    @Override
    public SloMetrics current() {
      _calls++;
      return _snapshot;
    }

    void at(long now) {
      _snapshot = new SloMetrics(now - 60000, now, now, 250, 20.0, 40.0, 50.0, 0, 0, 100, 100, 0, 0, _settings, true);
    }
  }
}
