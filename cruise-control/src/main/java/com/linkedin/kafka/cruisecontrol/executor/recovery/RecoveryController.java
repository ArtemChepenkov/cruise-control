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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.common.utils.Time;


/** Evaluates one execution's recovery control policy and publishes its latest snapshot. */
public final class RecoveryController {
  private final Time _time;
  private final SloMetricsProvider _metricsProvider;
  private final ThrottleControlPolicy _policy;
  private final AtomicReference<RecoveryControlState> _state;
  private RecoveryControlSettings _settings;
  private long _lastCheckMs = -1;
  private long _lastRateChangeMs = -1;
  private long _lastWindowEndMs = -1;
  private long _recoveredWindowStartMs = -1;
  private RecoveryProgressSnapshot _previousProgress;
  private int _healthyWindows;
  private int _noProgressWindows;

  /**
   * Start control state for a single execution.
   *
   * @param executionId the execution identifier
   * @param time the execution clock
   * @param settings the settings available before metrics arrive
   * @param metricsProvider the source of SLO snapshots
   * @param policy the policy to evaluate
   */
  public RecoveryController(String executionId, Time time, RecoveryControlSettings settings,
                            SloMetricsProvider metricsProvider, ThrottleControlPolicy policy) {
    _time = Objects.requireNonNull(time);
    _settings = Objects.requireNonNull(settings);
    _metricsProvider = Objects.requireNonNull(metricsProvider);
    _policy = Objects.requireNonNull(policy);
    _state = new AtomicReference<>(
        new RecoveryControlState(true, executionId, settings.initialThrottleBytesPerSecond(), 0, 0, null, List.of()));
  }

  /** @return the latest immutable state for this execution */
  public RecoveryControlState state() {
    return _state.get();
  }

  /** Stop evaluating the policy and retain the final snapshot for inspection. */
  public synchronized void stop() {
    RecoveryControlState state = _state.get();
    _state.set(new RecoveryControlState(false, state.executionId(), state.currentThrottleBytesPerSecond(),
                                        state.remainingBytes(), state.progressBytesPerSecond(), state.lastDecision(), state.warnings()));
  }

  /**
   * Evaluate a control cycle when the configured interval has elapsed.
   *
   * @param progress the most recent progress measurement
   * @param networkUtilizationRatio optional network utilization
   * @param diskUtilizationRatio optional disk utilization
   * @return a decision only if the execution must apply a different rate
   */
  public synchronized Optional<ControlDecision> onProgressCheck(RecoveryProgressSnapshot progress,
                                                               Double networkUtilizationRatio, Double diskUtilizationRatio) {
    RecoveryControlState state = _state.get();
    long now = _time.milliseconds();
    if (!state.enabled()) {
      return Optional.empty();
    }
    if (_lastCheckMs >= 0 && now - _lastCheckMs < _settings.controlIntervalMs()) {
      publish(state, progress, state.lastDecision());
      return Optional.empty();
    }

    _lastCheckMs = now;
    SloMetrics slo = readMetrics();
    now = _time.milliseconds();
    if (slo != null && slo.settings() != null && !slo.expiredAt(now, slo.settings().staleGraceMs())) {
      _settings = slo.settings();
    }
    boolean fresh = slo != null && slo.fresh() && !slo.expiredAt(now, _settings.staleGraceMs());
    boolean usable = fresh && slo.usable(_settings);
    updateProgressStreak(progress);
    if (usable && slo.windowEndMs() <= _lastWindowEndMs) {
      // The HTTP provider may return its cache. A cached window cannot authorize a second adjustment.
      publish(state, progress, state.lastDecision());
      return Optional.empty();
    }

    ControlDecision previous = state.lastDecision();
    ConstraintState previousState = previous == null ? null : previous.constraintState();
    long windowMs = 0;
    if (usable) {
      _lastWindowEndMs = slo.windowEndMs();
      windowMs = slo.windowEndMs() - slo.windowStartMs();
      if (previousState == ConstraintState.METRICS_STALE) {
        if (_recoveredWindowStartMs < 0) {
          _recoveredWindowStartMs = slo.windowEndMs();
        }
        windowMs = Math.min(windowMs, slo.windowEndMs() - _recoveredWindowStartMs);
      }
    } else {
      _recoveredWindowStartMs = -1;
    }
    if (usable && previousState != ConstraintState.METRICS_STALE && slo.healthy(_settings)) {
      _healthyWindows = Math.min(_settings.healthyWindows(), _healthyWindows + 1);
    } else {
      _healthyWindows = 0;
    }

    ControlInput input = new ControlInput(now, windowMs, _settings, slo, fresh, state.currentThrottleBytesPerSecond(),
                                          progress.remainingBytes(), progress.progressBytesPerSecond(),
                                          networkUtilizationRatio, diskUtilizationRatio, _healthyWindows, _noProgressWindows,
                                          previous == null ? null : previous.action(), previousState, _lastRateChangeMs);
    ControlDecision decision = decide(input);
    if (decision.constraintState() != ConstraintState.METRICS_STALE) {
      _recoveredWindowStartMs = -1;
    }
    publish(state, progress, decision);
    if (decision.throttleBytesPerSecond() == state.currentThrottleBytesPerSecond()) {
      return Optional.empty();
    }
    _lastRateChangeMs = now;
    return Optional.of(decision);
  }

  private SloMetrics readMetrics() {
    try {
      SloMetrics metrics = _metricsProvider.current();
      // With no observation, keep the decision input JSON-safe and preserve the missing value.
      return metrics != null && Double.isFinite(metrics.errorRate()) ? metrics : null;
    } catch (RuntimeException unavailable) {
      return null;
    }
  }

  private void updateProgressStreak(RecoveryProgressSnapshot progress) {
    if (_previousProgress != null && progress.timestampMs() <= _previousProgress.timestampMs()) {
      return;
    }
    if (progress.remainingBytes() > 0 && !progress.estimated()
        && _previousProgress != null && !_previousProgress.estimated()
        && progress.progressBytesPerSecond() < _settings.minProgressBytesPerSecond()) {
      _noProgressWindows = Math.min(_settings.noProgressWindows(), _noProgressWindows + 1);
    } else {
      _noProgressWindows = 0;
    }
    _previousProgress = progress;
  }

  private ControlDecision decide(ControlInput input) {
    try {
      ControlDecision decision = _policy.decide(input);
      if (decision.throttleBytesPerSecond() < _settings.minThrottleBytesPerSecond()
          || decision.throttleBytesPerSecond() > _settings.maxThrottleBytesPerSecond()) {
        throw new IllegalArgumentException("Policy throttle outside configured bounds");
      }
      return decision;
    } catch (RuntimeException policyFailure) {
      long safe = _settings.safeFallbackThrottleBytesPerSecond();
      long current = input.currentThrottleBytesPerSecond();
      ControlAction action = safe == current ? ControlAction.HOLD : safe < current ? ControlAction.DECREASE : ControlAction.INCREASE;
      return new ControlDecision(action, safe, ConstraintState.SLO_AT_RISK,
                                 "POLICY_FAILURE:" + policyFailure.getClass().getSimpleName(),
                                 List.of("POLICY_FAILURE"), input.timestampMs(), input);
    }
  }

  private void publish(RecoveryControlState old, RecoveryProgressSnapshot progress, ControlDecision decision) {
    List<String> warnings = new ArrayList<>(decision == null ? List.of() : decision.warnings());
    if (progress.estimated()) {
      warnings.add("PROGRESS_ESTIMATED");
    }
    long throttle = decision == null ? old.currentThrottleBytesPerSecond() : decision.throttleBytesPerSecond();
    _state.set(new RecoveryControlState(true, old.executionId(), throttle, progress.remainingBytes(),
                                        progress.progressBytesPerSecond(), decision, warnings));
  }
}
