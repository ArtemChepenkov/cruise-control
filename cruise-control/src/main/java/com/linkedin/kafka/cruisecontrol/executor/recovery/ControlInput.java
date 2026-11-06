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

/** Measurements and previous decision metadata used to evaluate one control cycle. */
public record ControlInput(
    long timestampMs,
    long windowMs,
    RecoveryControlSettings settings,
    SloMetrics slo,
    boolean metricsFresh,
    long currentThrottleBytesPerSecond,
    long remainingBytes,
    long progressBytesPerSecond,
    Double networkUtilizationRatio,
    Double diskUtilizationRatio,
    int consecutiveHealthyWindows,
    int consecutiveNoProgressWindows,
    ControlAction previousAction,
    ConstraintState previousConstraintState,
    long previousDecisionTimestampMs) {
}
