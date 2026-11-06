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

import com.github.tomakehurst.wiremock.junit.WireMockRule;
import java.net.URI;
import java.time.Instant;
import java.util.Map;
import org.apache.kafka.common.utils.MockTime;
import org.junit.Rule;
import org.junit.Test;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.okJson;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;


/** Exercises the Load Generator HTTP contract against an actual HTTP listener. */
public class HttpSloMetricsProviderTest {
  private static final String PATH = "/api/load/metrics/current";
  private static final long NOW = Instant.parse("2026-10-29T10:00:00Z").toEpochMilli();

  @Rule
  public WireMockRule _server = new WireMockRule(0);
  private final MockTime _time = new MockTime(0, NOW, 0);

  @Test
  public void testFreshMetricsAndSettingsFromLoadGenerator() {
    stub(metrics(NOW - 1000));
    SloMetrics snapshot = provider().current();

    assertTrue(snapshot.fresh());
    assertEquals(NOW - 1000, snapshot.generatedAtMs());
    assertEquals(NOW - 61000, snapshot.windowStartMs());
    assertEquals(250, snapshot.requestCount());
    assertEquals(74.0, snapshot.p99Ms(), 0.0);
    assertEquals(0.008, snapshot.errorRate(), 0.0);
    assertEquals(125.0, snapshot.settings().produceP99Ms(), 0.0);
    assertEquals(24 * 1024L * 1024, snapshot.settings().initialThrottleBytesPerSecond());
    assertEquals(10 * 1024L * 1024, snapshot.settings().safeFallbackThrottleBytesPerSecond());
    assertEquals(15000, snapshot.settings().controlIntervalMs());
    assertEquals(60000, snapshot.settings().metricsWindowMs());
  }

  @Test
  public void testConfiguredProviderUsesExecutorDefaults() {
    stub(metrics(NOW));
    HttpSloMetricsProvider provider = new HttpSloMetricsProvider(_time);
    provider.configure(Map.of("bootstrap.servers", "localhost:9092",
                              "recovery.control.slo.url", _server.baseUrl() + PATH,
                              "recovery.control.safe.fallback.throttle.bytes.per.second", "12582912"));
    assertEquals(12582912, provider.current().settings().safeFallbackThrottleBytesPerSecond());
  }

  @Test
  public void testTimeoutUsesCacheUntilFreshnessExpires() {
    HttpSloMetricsProvider provider = provider();
    stub(metrics(NOW));
    assertTrue(provider.current().fresh());

    _server.resetAll();
    _server.stubFor(get(urlEqualTo(PATH)).willReturn(okJson(metrics(NOW + 1000)).withFixedDelay(6000)));
    _time.sleep(10000);
    SloMetrics cached = provider.current();
    assertTrue(cached.fresh());
    assertEquals(NOW, cached.generatedAtMs());

    _server.resetAll();
    _server.stubFor(get(urlEqualTo(PATH)).willReturn(aResponse().withStatus(503)));
    _time.sleep(20001);
    SloMetrics stale = provider.current();
    assertFalse(stale.fresh());
    assertEquals(cached.p99Ms(), stale.p99Ms());
    assertEquals(cached.errorRate(), stale.errorRate(), 0.0);
  }

  @Test
  public void testEmptyWindowAndUnavailableMetricsAreNotFresh() {
    HttpSloMetricsProvider provider = provider();
    _server.stubFor(get(urlEqualTo(PATH)).willReturn(aResponse().withStatus(503)));
    SloMetrics unavailable = provider.current();
    assertFalse(unavailable.fresh());
    assertNull(unavailable.p99Ms());
    assertTrue(Double.isNaN(unavailable.errorRate()));

    _server.resetAll();
    stub(metrics(NOW).replace("\"requestCount\": 250", "\"requestCount\": 0")
                     .replace("\"errorCount\": 2", "\"errorCount\": 0")
                     .replace("\"errorRate\": 0.008", "\"errorRate\": 0")
                     .replace("\"p99Ms\": 74.0", "\"p99Ms\": null"));
    SloMetrics empty = provider.current();
    assertFalse(empty.fresh());
    assertEquals(0, empty.requestCount());
    assertNull(empty.p99Ms());
  }

  @Test
  public void testInvalidSettingsDoNotReplaceLastGoodSnapshot() {
    HttpSloMetricsProvider provider = provider();
    stub(metrics(NOW - 1000));
    SloMetrics good = provider.current();

    _server.resetAll();
    stub(metrics(NOW).replace("\"initialThrottleMiBps\": 24", "\"initialThrottleMiBps\": 200"));
    assertEquals(good, provider.current());
  }

  @Test
  public void testAllFailedRequestsStillProvideFreshErrorMetrics() {
    stub(metrics(NOW).replace("\"errorCount\": 2", "\"errorCount\": 250")
                     .replace("\"errorRate\": 0.008", "\"errorRate\": 1")
                     .replace("\"p99Ms\": 74.0", "\"p99Ms\": null"));
    SloMetrics snapshot = provider().current();
    assertTrue(snapshot.fresh());
    assertNull(snapshot.p99Ms());
    assertEquals(1.0, snapshot.errorRate(), 0.0);
  }

  @Test
  public void testFutureTimestampCannotBecomeFresh() {
    stub(metrics(NOW + 1000));
    assertFalse(provider().current().fresh());
  }

  private HttpSloMetricsProvider provider() {
    return new HttpSloMetricsProvider(_time, URI.create(_server.baseUrl() + PATH),
                                      AimdThrottleControlPolicyTest.settings());
  }

  private void stub(String body) {
    _server.stubFor(get(urlEqualTo(PATH)).willReturn(okJson(body)));
  }

  private static String metrics(long endMs) {
    return """
        {
          "windowStart": "WINDOW_START", "windowEnd": "WINDOW_END", "generatedAt": "WINDOW_END",
          "requestCount": 250, "p50Ms": 12.0, "p95Ms": 60.0, "p99Ms": 74.0,
          "errorCount": 2, "errorRate": 0.008, "offeredRps": 100.0,
          "achievedRps": 4.13, "bytesSent": 253952, "outstandingRequests": 3,
          "controlSettings": {
            "slo": { "produceP99Ms": 125.0, "maxErrorRate": 0.02 },
            "recovery": { "initialThrottleMiBps": 24, "controlInterval": "PT15S" }
          }
        }
        """.replace("WINDOW_START", Instant.ofEpochMilli(endMs - 60000).toString())
           .replace("WINDOW_END", Instant.ofEpochMilli(endMs).toString());
  }
}
