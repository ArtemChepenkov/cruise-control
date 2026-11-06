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
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.common.config.ConfigException;
import org.junit.Test;

import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;


public class RecoveryControlConfigTest {
  private static final long MIB = 1024L * 1024;
  private static final String PREFIX = "recovery.control.";
  private static final String INITIAL = PREFIX + "initial.throttle.bytes.per.second";
  private static final String MIN = PREFIX + "min.throttle.bytes.per.second";
  private static final String MAX = PREFIX + "max.throttle.bytes.per.second";
  private static final String FALLBACK = PREFIX + "safe.fallback.throttle.bytes.per.second";
  private static final String PROGRESS_FLOOR = PREFIX + "progress.floor.bytes.per.second";

  @Test
  public void testDefaults() {
    KafkaCruiseControlConfig config = configWith(Map.of());

    assertFalse(config.getBoolean(PREFIX + "enabled"));
    assertEquals(10000L, config.getLong(PREFIX + "interval.ms").longValue());
    assertEquals(60000L, config.getLong(PREFIX + "metrics.window.ms").longValue());
    assertEquals(30000L, config.getLong(PREFIX + "metrics.stale.grace.ms").longValue());
    assertEquals(20 * MIB, config.getLong(INITIAL).longValue());
    assertEquals(MIB, config.getLong(MIN).longValue());
    assertEquals(100 * MIB, config.getLong(MAX).longValue());
    assertEquals(5 * MIB, config.getLong(PREFIX + "additive.step.bytes.per.second").longValue());
    assertEquals(20 * MIB, config.getLong(PREFIX + "max.change.bytes.per.second").longValue());
    assertEquals(0.7, config.getDouble(PREFIX + "multiplicative.decrease"), 0.0);
    assertEquals(0.85, config.getDouble(PREFIX + "healthy.slo.ratio"), 0.0);
    assertEquals(2, config.getInt(PREFIX + "healthy.windows").intValue());
    assertEquals(20000L, config.getLong(PREFIX + "cooldown.ms").longValue());
    assertEquals(100L, config.getLong(PREFIX + "minimum.observations").longValue());
    assertEquals(config.getLong(INITIAL), config.getLong(FALLBACK));
    assertEquals(5 * MIB, config.getLong(PROGRESS_FLOOR).longValue());
    assertEquals(MIB, config.getLong(PREFIX + "min.progress.bytes.per.second").longValue());
    assertEquals(3, config.getInt(PREFIX + "no.progress.windows").intValue());
  }

  @Test
  public void testFallbackInheritsConfiguredInitialThrottle() {
    KafkaCruiseControlConfig config = configWith(Map.of(INITIAL, Long.toString(30 * MIB)));
    assertEquals(30 * MIB, config.getLong(FALLBACK).longValue());
  }

  @Test
  public void testSettingsFromConfig() {
    KafkaCruiseControlConfig config = configWith(Map.of(
        INITIAL, 30 * MIB, PROGRESS_FLOOR, 6 * MIB, PREFIX + "metrics.window.ms", 45000L));
    RecoveryControlSettings expected = new RecoveryControlSettings(
        120.0, 0.02, 30 * MIB, MIB, 100 * MIB, 30 * MIB, 5 * MIB, 20 * MIB, 0.7, 0.85, 2, 100,
        20000, 10000, 45000, 30000, 6 * MIB, MIB, 3);
    assertEquals(expected, RecoveryControlSettings.fromConfig(config, 120.0, 0.02));
  }

  @Test
  public void testExplicitFallbackAndEqualThrottleBounds() {
    KafkaCruiseControlConfig config = configWith(Map.of(
        MIN, 5 * MIB, INITIAL, 5 * MIB, MAX, 5 * MIB, FALLBACK, 5 * MIB,
        PREFIX + "healthy.slo.ratio", 1.0));
    assertEquals(5 * MIB, config.getLong(FALLBACK).longValue());

    config = configWith(Map.of(INITIAL, 30 * MIB, FALLBACK, 10 * MIB));
    assertEquals(10 * MIB, config.getLong(FALLBACK).longValue());
  }

  @Test
  public void testRejectsInvalidThrottleBounds() {
    assertInvalidValue(INITIAL, MIB - 1);
    assertInvalidValue(INITIAL, 100 * MIB + 1);
    assertInvalidValue(FALLBACK, MIB - 1);
    assertInvalidValue(FALLBACK, 100 * MIB + 1);
    assertInvalidValue(PROGRESS_FLOOR, 100 * MIB + 1);
  }

  @Test
  public void testRejectsInvalidScalarSettings() {
    assertInvalidValue(PREFIX + "interval.ms", 0);
    assertInvalidValue(PREFIX + "minimum.observations", -1);
    assertInvalidValue(PREFIX + "no.progress.windows", 0);
    assertInvalidValue(PREFIX + "multiplicative.decrease", 0.0);
    assertInvalidValue(PREFIX + "multiplicative.decrease", 1.0);
    assertInvalidValue(PREFIX + "multiplicative.decrease", Double.NaN);
    assertInvalidValue(PREFIX + "healthy.slo.ratio", 0.0);
    assertInvalidValue(PREFIX + "healthy.slo.ratio", 1.01);
    assertInvalidValue(PREFIX + "healthy.slo.ratio", Double.POSITIVE_INFINITY);
  }

  private static void assertInvalidValue(String key, Object value) {
    ConfigException error = assertThrows(ConfigException.class, () -> configWith(Map.of(key, value)));
    assertTrue(error.getMessage(), error.getMessage().contains(key));
  }

  private static KafkaCruiseControlConfig configWith(Map<String, ?> overrides) {
    Properties properties = getKafkaCruiseControlProperties();
    properties.putAll(overrides);
    return new KafkaCruiseControlConfig(properties);
  }
}
