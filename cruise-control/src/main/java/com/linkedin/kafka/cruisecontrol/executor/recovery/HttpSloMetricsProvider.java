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

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.ExecutorConfig;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.DateTimeException;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.common.utils.Time;


/** Reads the Load Generator's JSON and retains the last valid snapshot across HTTP failures. */
public final class HttpSloMetricsProvider implements SloMetricsProvider {
  private static final long MIB = 1024L * 1024;
  private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(5);
  private static final ObjectMapper MAPPER = new ObjectMapper().enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);

  private final Time _time;
  private final HttpClient _client;
  private HttpRequest _request;
  private RecoveryControlSettings _defaults;
  private SloMetrics _cached;

  /** Create a provider for Cruise Control's configured-instance API. */
  public HttpSloMetricsProvider() {
    this(Time.SYSTEM);
  }

  /**
   * Create a provider for configuration with an injected clock.
   *
   * @param time the clock used to check freshness
   */
  public HttpSloMetricsProvider(Time time) {
    _time = Objects.requireNonNull(time);
    _client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3)).build();
  }

  /**
   * Create a provider with an execution's endpoint and default settings.
   *
   * @param time the clock used to check freshness
   * @param endpoint the full Load Generator metrics URL
   * @param defaults settings used for omitted fields
   */
  public HttpSloMetricsProvider(Time time, URI endpoint, RecoveryControlSettings defaults) {
    this(time);
    _request = request(endpoint);
    _defaults = Objects.requireNonNull(defaults);
  }

  @Override
  public synchronized void configure(Map<String, ?> configs) {
    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(configs);
    String url = config.getString(ExecutorConfig.RECOVERY_CONTROL_SLO_URL_CONFIG);
    if (url == null || url.isBlank()) {
      throw new IllegalArgumentException(ExecutorConfig.RECOVERY_CONTROL_SLO_URL_CONFIG + " must specify the metrics endpoint");
    }
    HttpRequest request = request(URI.create(url));
    _defaults = RecoveryControlSettings.fromConfig(config, Double.NaN, Double.NaN);
    _request = request;
    _cached = null;
  }

  @Override
  public synchronized SloMetrics current() {
    if (_request == null) {
      throw new IllegalStateException("SLO metrics provider has no endpoint");
    }
    try {
      SloMetrics snapshot = fetch();
      if (snapshot != null && snapshot.generatedAtMs() <= _time.milliseconds()
          && (_cached == null || snapshot.generatedAtMs() >= _cached.generatedAtMs())) {
        _cached = snapshot;
      }
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
    } catch (IOException | ExecutionException | TimeoutException | IllegalArgumentException | DateTimeException | ArithmeticException ignored) {
      // A failed request or invalid response leaves the last valid snapshot available.
    }
    if (_cached == null) {
      return new SloMetrics(0, 0, 0, 0, null, null, null, 0, Double.NaN, 0, 0, 0, 0, null, false);
    }
    long age = _time.milliseconds() - _cached.generatedAtMs();
    boolean fresh = age >= 0 && age <= _cached.settings().staleGraceMs() && _cached.requestCount() > 0;
    return new SloMetrics(_cached.windowStartMs(), _cached.windowEndMs(), _cached.generatedAtMs(), _cached.requestCount(),
                          _cached.p50Ms(), _cached.p95Ms(), _cached.p99Ms(), _cached.errorCount(), _cached.errorRate(),
                          _cached.offeredRps(), _cached.achievedRps(), _cached.bytesSent(), _cached.outstandingRequests(),
                          _cached.settings(), fresh);
  }

  private SloMetrics fetch() throws IOException, ExecutionException, InterruptedException, TimeoutException {
    var pending = _client.sendAsync(_request, HttpResponse.BodyHandlers.ofString());
    HttpResponse<String> response;
    try {
      response = pending.get(REQUEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    } catch (InterruptedException | TimeoutException interruptedOrTimedOut) {
      pending.cancel(true);
      throw interruptedOrTimedOut;
    }
    return response.statusCode() == 200 ? parse(MAPPER.readTree(response.body())) : null;
  }

  private SloMetrics parse(JsonNode metrics) {
    if (metrics == null || !metrics.isObject()) {
      throw new IllegalArgumentException("Expected a metrics object");
    }
    long start = instant(metrics, "windowStart");
    long end = instant(metrics, "windowEnd");
    long generated = instant(metrics, "generatedAt");
    long requests = integer(metrics, "requestCount", -1);
    long errors = integer(metrics, "errorCount", -1);
    double errorRate = number(metrics, "errorRate", Double.NaN);
    boolean hasSuccessfulRequests = requests > errors;
    Double p99 = hasSuccessfulRequests ? percentile(metrics, "p99Ms") : null;
    if (start > end || end > generated || requests < 0 || errors < 0 || errors > requests || errorRate > 1) {
      throw new IllegalArgumentException("Invalid metrics window");
    }
    return new SloMetrics(start, end, generated, requests,
                          hasSuccessfulRequests ? percentile(metrics, "p50Ms") : null,
                          hasSuccessfulRequests ? percentile(metrics, "p95Ms") : null, p99,
                          errors, errorRate, number(metrics, "offeredRps", Double.NaN),
                          number(metrics, "achievedRps", Double.NaN), integer(metrics, "bytesSent", -1),
                          integer(metrics, "outstandingRequests", -1), settings(object(metrics, "controlSettings")), false);
  }

  private RecoveryControlSettings settings(JsonNode control) {
    JsonNode slo = object(control, "slo");
    JsonNode recovery = object(control, "recovery");
    long initial = bytes(recovery, "initialThrottleMiBps", _defaults.initialThrottleBytesPerSecond());
    RecoveryControlSettings settings = new RecoveryControlSettings(
        number(slo, "produceP99Ms", _defaults.produceP99Ms()),
        number(slo, "maxErrorRate", _defaults.maxErrorRate()),
        initial,
        bytes(recovery, "minThrottleMiBps", _defaults.minThrottleBytesPerSecond()),
        bytes(recovery, "maxThrottleMiBps", _defaults.maxThrottleBytesPerSecond()),
        bytes(recovery, "safeFallbackThrottleMiBps", _defaults.safeFallbackThrottleBytesPerSecond()),
        bytes(recovery, "additiveStepMiBps", _defaults.additiveStepBytesPerSecond()),
        bytes(recovery, "maxChangeMiBps", _defaults.maxChangeBytesPerSecond()),
        number(recovery, "multiplicativeDecrease", _defaults.multiplicativeDecrease()),
        number(recovery, "healthySloRatio", _defaults.healthySloRatio()),
        Math.toIntExact(integer(recovery, "healthyWindows", _defaults.healthyWindows())),
        integer(recovery, "minimumObservations", _defaults.minimumObservations()),
        duration(recovery, "cooldown", _defaults.cooldownMs()),
        duration(recovery, "controlInterval", _defaults.controlIntervalMs()),
        duration(recovery, "metricsWindow", _defaults.metricsWindowMs()),
        duration(recovery, "staleGrace", _defaults.staleGraceMs()),
        bytes(recovery, "progressFloorMiBps", _defaults.progressFloorBytesPerSecond()),
        bytes(recovery, "minProgressMiBps", _defaults.minProgressBytesPerSecond()),
        Math.toIntExact(integer(recovery, "noProgressWindows", _defaults.noProgressWindows())));
    validateSettings(settings);
    return settings;
  }

  private static void validateSettings(RecoveryControlSettings settings) {
    long min = settings.minThrottleBytesPerSecond();
    long max = settings.maxThrottleBytesPerSecond();
    if (min <= 0 || !within(settings.initialThrottleBytesPerSecond(), min, max)
        || !within(settings.safeFallbackThrottleBytesPerSecond(), min, max)
        || !within(settings.progressFloorBytesPerSecond(), min, max)
        || settings.additiveStepBytesPerSecond() <= 0 || settings.maxChangeBytesPerSecond() <= 0
        || settings.minProgressBytesPerSecond() <= 0 || settings.healthyWindows() <= 0
        || settings.noProgressWindows() <= 0 || settings.minimumObservations() <= 0
        || settings.cooldownMs() <= 0 || settings.controlIntervalMs() <= 0 || settings.metricsWindowMs() <= 0
        || settings.staleGraceMs() <= 0 || settings.produceP99Ms() <= 0 || settings.maxErrorRate() > 1
        || !(settings.multiplicativeDecrease() > 0 && settings.multiplicativeDecrease() < 1)
        || !(settings.healthySloRatio() > 0 && settings.healthySloRatio() <= 1)) {
      throw new IllegalArgumentException("Invalid recovery control settings");
    }
  }

  private static boolean within(long value, long min, long max) {
    return value >= min && value <= max;
  }

  private static HttpRequest request(URI endpoint) {
    return HttpRequest.newBuilder(endpoint).timeout(REQUEST_TIMEOUT).GET().build();
  }

  private static JsonNode object(JsonNode parent, String field) {
    JsonNode value = parent.path(field);
    if (value.isMissingNode() || value.isObject()) {
      return value;
    }
    throw new IllegalArgumentException("Expected an object for " + field);
  }

  private static String string(JsonNode parent, String field) {
    JsonNode value = parent.path(field);
    if (!value.isTextual()) {
      throw new IllegalArgumentException("Expected text for " + field);
    }
    return value.textValue();
  }

  private static long instant(JsonNode parent, String field) {
    return Instant.parse(string(parent, field)).toEpochMilli();
  }

  private static long integer(JsonNode parent, String field, long defaultValue) {
    JsonNode value = parent.path(field);
    long result = value.isMissingNode() ? defaultValue
        : value.isIntegralNumber() && value.canConvertToLong() ? value.longValue() : -1;
    if (result < 0) {
      throw new IllegalArgumentException("Expected a nonnegative integer for " + field);
    }
    return result;
  }

  private static double number(JsonNode parent, String field, double defaultValue) {
    JsonNode value = parent.path(field);
    double result = value.isMissingNode() ? defaultValue : value.isNumber() ? value.doubleValue() : Double.NaN;
    if (!Double.isFinite(result) || result < 0) {
      throw new IllegalArgumentException("Expected a nonnegative number for " + field);
    }
    return result;
  }

  private static Double percentile(JsonNode parent, String field) {
    return parent.path(field).isNull() ? null : number(parent, field, Double.NaN);
  }

  private static long bytes(JsonNode parent, String field, long defaultValue) {
    return parent.has(field) ? Math.multiplyExact(integer(parent, field, -1), MIB) : defaultValue;
  }

  private static long duration(JsonNode parent, String field, long defaultValue) {
    return parent.has(field) ? Duration.parse(string(parent, field)).toMillis() : defaultValue;
  }
}
