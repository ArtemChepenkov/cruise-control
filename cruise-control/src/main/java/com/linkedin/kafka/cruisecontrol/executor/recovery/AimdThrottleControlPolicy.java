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


/** Additive increase and multiplicative decrease based on user SLO and recovery progress. */
public final class AimdThrottleControlPolicy implements ThrottleControlPolicy {
  @Override
  public void configure(Map<String, ?> configs) {
    // Every evaluation receives the effective settings for its execution.
  }

  @Override
  public ControlDecision decide(ControlInput input) {
    RecoveryControlSettings settings = input.settings();
    SloMetrics slo = input.slo();
    long current = input.currentThrottleBytesPerSecond();
    if (slo == null || slo.expiredAt(input.timestampMs(), settings.staleGraceMs())
        || (!input.metricsFresh() && slo.requestCount() > 0)) {
      return decision(input, settings.safeFallbackThrottleBytesPerSecond(), false,
                      ConstraintState.METRICS_STALE, "METRICS_STALE", List.of("METRICS_STALE"));
    }
    if (!slo.usable(settings) || !input.metricsFresh()) {
      return decision(input, current, false, ConstraintState.METRICS_STALE,
                      "INSUFFICIENT_OBSERVATIONS", List.of("METRICS_STALE"));
    }

    boolean badLatency = slo.p99Ms() != null && slo.p99Ms() > settings.produceP99Ms();
    boolean badErrors = slo.errorRate() > settings.maxErrorRate();
    boolean violated = badLatency || badErrors;
    ConstraintState state = violated ? ConstraintState.SLO_AT_RISK : ConstraintState.SATISFIED;
    if (input.previousConstraintState() == ConstraintState.METRICS_STALE) {
      ConstraintState waiting = input.windowMs() < settings.metricsWindowMs() ? ConstraintState.METRICS_STALE : state;
      return decision(input, current, false, waiting, "METRICS_RECOVERED_WAIT", List.of());
    }

    long floor = settings.progressFloorBytesPerSecond();
    boolean recovering = input.remainingBytes() > 0;
    boolean stalled = recovering && input.consecutiveNoProgressWindows() >= settings.noProgressWindows();
    if (violated && recovering && current == floor) {
      return decision(input, floor, false, ConstraintState.CONSTRAINTS_UNSATISFIABLE,
                      "UNSATISFIABLE", List.of("CONSTRAINTS_UNSATISFIABLE"));
    }
    if (stalled && current < floor) {
      return decision(input, floor, true, state, "NO_PROGRESS", List.of());
    }
    if (input.previousDecisionTimestampMs() >= 0
        && input.timestampMs() - input.previousDecisionTimestampMs() < settings.cooldownMs()) {
      return decision(input, current, false, state, "COOLDOWN", List.of());
    }
    if (violated) {
      long reduced = Math.round(current * settings.multiplicativeDecrease());
      if (stalled) {
        reduced = Math.max(floor, reduced);
      }
      return decision(input, reduced, false, state, badLatency ? "SLO_P99_EXCEEDED" : "SLO_ERRORS_EXCEEDED", List.of());
    }
    if (!slo.healthy(settings)) {
      return decision(input, current, false, state, "HYSTERESIS", List.of());
    }
    long requested = input.consecutiveHealthyWindows() >= settings.healthyWindows()
        ? add(current, settings.additiveStepBytesPerSecond()) : current;
    return decision(input, requested, false, state, "SLO_HEALTHY", List.of());
  }

  private static ControlDecision decision(ControlInput input, long requested, boolean progressGuard,
                                          ConstraintState state, String reason, List<String> warnings) {
    RecoveryControlSettings settings = input.settings();
    long current = input.currentThrottleBytesPerSecond();
    long step = settings.maxChangeBytesPerSecond();
    long stepped = Math.min(add(current, step), Math.max(Math.max(0, current - step), requested));
    long rate = Math.min(settings.maxThrottleBytesPerSecond(), Math.max(settings.minThrottleBytesPerSecond(), stepped));
    ControlAction action = ControlAction.HOLD;
    if (rate > current) {
      action = progressGuard ? ControlAction.PROGRESS_GUARD : ControlAction.INCREASE;
    } else if (rate < current) {
      action = ControlAction.DECREASE;
    }
    return new ControlDecision(action, rate, state, reason, warnings, input.timestampMs(), input);
  }

  private static long add(long rate, long increment) {
    return rate > Long.MAX_VALUE - increment ? Long.MAX_VALUE : rate + increment;
  }
}
