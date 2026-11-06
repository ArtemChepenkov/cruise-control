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

/** One window of user-facing metrics. Latency percentiles may be absent in an empty window. */
public record SloMetrics(
    long windowStartMs,
    long windowEndMs,
    long generatedAtMs,
    long requestCount,
    Double p50Ms,
    Double p95Ms,
    Double p99Ms,
    long errorCount,
    double errorRate,
    double offeredRps,
    double achievedRps,
    long bytesSent,
    long outstandingRequests,
    RecoveryControlSettings settings,
    boolean fresh) {
}
