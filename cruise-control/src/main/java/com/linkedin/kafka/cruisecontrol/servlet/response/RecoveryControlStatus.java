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

package com.linkedin.kafka.cruisecontrol.servlet.response;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.linkedin.cruisecontrol.servlet.parameters.CruiseControlParameters;
import com.linkedin.kafka.cruisecontrol.executor.recovery.ControlDecision;
import com.linkedin.kafka.cruisecontrol.executor.recovery.RecoveryControlState;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.linkedin.kafka.cruisecontrol.servlet.response.ResponseUtils.JSON_VERSION;
import static com.linkedin.kafka.cruisecontrol.servlet.response.ResponseUtils.VERSION;


/** HTTP representation of a recovery control snapshot. */
@JsonResponseClass
public final class RecoveryControlStatus extends AbstractCruiseControlResponse {
  private static final Gson GSON = new GsonBuilder().serializeNulls().create();
  @JsonResponseField
  private static final String ENABLED = "enabled";
  @JsonResponseField
  private static final String EXECUTION_ID = "executionId";
  @JsonResponseField
  private static final String THROTTLE = "currentThrottleBytesPerSecond";
  @JsonResponseField
  private static final String REMAINING = "remainingBytes";
  @JsonResponseField
  private static final String PROGRESS = "progressBytesPerSecond";
  @JsonResponseField
  private static final String LAST_ACTION = "lastAction";
  @JsonResponseField
  private static final String CONSTRAINT_STATE = "constraintState";
  @JsonResponseField
  private static final String REASON = "reason";
  @JsonResponseField
  private static final String WARNINGS = "warnings";
  @JsonResponseField
  private static final String DECISION_TIMESTAMP_MS = "decisionTimestampMs";

  private final RecoveryControlState _state;

  /**
   * Create the response for an execution snapshot.
   *
   * @param state the active execution state, or null when inactive
   */
  public RecoveryControlStatus(RecoveryControlState state) {
    super(null);
    _state = state;
  }

  @Override
  protected void discardIrrelevantAndCacheRelevant(CruiseControlParameters parameters) {
    if (parameters.json()) {
      _cachedResponse = GSON.toJson(getJsonStructure());
    } else {
      _cachedResponse = enabled()
          ? String.format("Recovery control for %s at %d bytes/second.",
                          _state.executionId(), _state.currentThrottleBytesPerSecond())
          : "Recovery control is inactive.";
    }
  }

  private boolean enabled() {
    return _state != null && _state.enabled();
  }

  private Map<String, Object> getJsonStructure() {
    boolean enabled = enabled();
    ControlDecision decision = enabled ? _state.lastDecision() : null;
    Map<String, Object> fields = new LinkedHashMap<>();
    fields.put(VERSION, JSON_VERSION);
    fields.put(ENABLED, enabled);
    fields.put(EXECUTION_ID, enabled ? _state.executionId() : null);
    fields.put(THROTTLE, enabled ? _state.currentThrottleBytesPerSecond() : null);
    fields.put(REMAINING, enabled ? _state.remainingBytes() : null);
    fields.put(PROGRESS, enabled ? _state.progressBytesPerSecond() : null);
    fields.put(LAST_ACTION, decision == null ? null : decision.action().name());
    fields.put(CONSTRAINT_STATE, decision == null ? null : decision.constraintState().name());
    fields.put(REASON, decision == null ? null : decision.reason());
    fields.put(WARNINGS, enabled ? _state.warnings() : List.of());
    fields.put(DECISION_TIMESTAMP_MS, decision == null ? null : decision.timestampMs());
    return fields;
  }
}
