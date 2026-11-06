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

import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.ExecutorConfig;


/** SLO limits and recovery policy parameters for an execution. */
public record RecoveryControlSettings(
    double produceP99Ms,
    double maxErrorRate,
    long initialThrottleBytesPerSecond,
    long minThrottleBytesPerSecond,
    long maxThrottleBytesPerSecond,
    long safeFallbackThrottleBytesPerSecond,
    long additiveStepBytesPerSecond,
    long maxChangeBytesPerSecond,
    double multiplicativeDecrease,
    double healthySloRatio,
    int healthyWindows,
    long minimumObservations,
    long cooldownMs,
    long controlIntervalMs,
    long metricsWindowMs,
    long staleGraceMs,
    long progressFloorBytesPerSecond,
    long minProgressBytesPerSecond,
    int noProgressWindows) {
  /**
   * Combine the execution's SLO limits with the parsed Cruise Control configuration.
   *
   * @param config the validated Cruise Control configuration
   * @param produceP99Ms the produce latency limit in milliseconds
   * @param maxErrorRate the maximum fraction of requests that can fail
   * @return recovery control settings for the execution
   */
  public static RecoveryControlSettings fromConfig(KafkaCruiseControlConfig config, double produceP99Ms, double maxErrorRate) {
    return new RecoveryControlSettings(
        produceP99Ms,
        maxErrorRate,
        config.getLong(ExecutorConfig.RECOVERY_CONTROL_INITIAL_THROTTLE_CONFIG),
        config.getLong(ExecutorConfig.RECOVERY_CONTROL_MIN_THROTTLE_CONFIG),
        config.getLong(ExecutorConfig.RECOVERY_CONTROL_MAX_THROTTLE_CONFIG),
        config.getLong(ExecutorConfig.RECOVERY_CONTROL_SAFE_FALLBACK_THROTTLE_CONFIG),
        config.getLong(ExecutorConfig.RECOVERY_CONTROL_ADDITIVE_STEP_CONFIG),
        config.getLong(ExecutorConfig.RECOVERY_CONTROL_MAX_CHANGE_CONFIG),
        config.getDouble(ExecutorConfig.RECOVERY_CONTROL_MULTIPLICATIVE_DECREASE_CONFIG),
        config.getDouble(ExecutorConfig.RECOVERY_CONTROL_HEALTHY_SLO_RATIO_CONFIG),
        config.getInt(ExecutorConfig.RECOVERY_CONTROL_HEALTHY_WINDOWS_CONFIG),
        config.getLong(ExecutorConfig.RECOVERY_CONTROL_MINIMUM_OBSERVATIONS_CONFIG),
        config.getLong(ExecutorConfig.RECOVERY_CONTROL_COOLDOWN_MS_CONFIG),
        config.getLong(ExecutorConfig.RECOVERY_CONTROL_INTERVAL_MS_CONFIG),
        config.getLong(ExecutorConfig.RECOVERY_CONTROL_METRICS_WINDOW_MS_CONFIG),
        config.getLong(ExecutorConfig.RECOVERY_CONTROL_METRICS_STALE_GRACE_MS_CONFIG),
        config.getLong(ExecutorConfig.RECOVERY_CONTROL_PROGRESS_FLOOR_CONFIG),
        config.getLong(ExecutorConfig.RECOVERY_CONTROL_MIN_PROGRESS_CONFIG),
        config.getInt(ExecutorConfig.RECOVERY_CONTROL_NO_PROGRESS_WINDOWS_CONFIG));
  }
}
