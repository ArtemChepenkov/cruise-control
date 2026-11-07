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

package com.linkedin.kafka.cruisecontrol.servlet;

import com.codahale.metrics.MetricRegistry;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.linkedin.cruisecontrol.http.CruiseControlRequestContext;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlRequestHandler;
import com.linkedin.kafka.cruisecontrol.async.AsyncKafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.RequestParameterWrapper;
import com.linkedin.kafka.cruisecontrol.executor.recovery.ConstraintState;
import com.linkedin.kafka.cruisecontrol.executor.recovery.ControlAction;
import com.linkedin.kafka.cruisecontrol.executor.recovery.ControlDecision;
import com.linkedin.kafka.cruisecontrol.executor.recovery.RecoveryControlState;
import com.linkedin.kafka.cruisecontrol.servlet.handler.sync.RecoveryControlRequest;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.RecoveryControlParameters;
import com.linkedin.kafka.cruisecontrol.servlet.response.ResponseUtils;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import javax.servlet.http.HttpServletRequest;
import org.easymock.Capture;
import org.junit.Test;

import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties;
import static com.linkedin.kafka.cruisecontrol.servlet.CruiseControlEndPoint.RECOVERY_CONTROL;
import static com.linkedin.kafka.cruisecontrol.servlet.KafkaCruiseControlServletUtils.KAFKA_CRUISE_CONTROL_HTTP_SERVLET_REQUEST_OBJECT_CONFIG;
import static com.linkedin.kafka.cruisecontrol.servlet.KafkaCruiseControlServletUtils.requestParameterFor;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.newCapture;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


/** Tests registration and read-only HTTP routing for recovery status. */
public class RecoveryControlEndpointTest {
  private static final String PATH = "/kafkacruisecontrol/recovery_control";
  private final KafkaCruiseControlConfig _config = new KafkaCruiseControlConfig(getKafkaCruiseControlProperties());

  @Test
  public void testRecoveryControlRegisteredForGetOnly() {
    RequestParameterWrapper mapping = requestParameterFor(RECOVERY_CONTROL);
    assertEquals(RecoveryControlRequest.class, _config.getClass(mapping.requestClass()));
    assertEquals(RecoveryControlParameters.class, _config.getClass(mapping.parametersClass()));
    assertTrue(CruiseControlEndPoint.getEndpoints().contains(RECOVERY_CONTROL));
    assertFalse(CruiseControlEndPoint.postEndpoints().contains(RECOVERY_CONTROL));
  }

  @Test
  public void testDisabledStatusReportsNullExecutionId() throws IOException {
    String response = getStatus(null);
    JsonObject json = JsonParser.parseString(response).getAsJsonObject();
    assertFalse(json.get("enabled").getAsBoolean());
    assertTrue(json.get("executionId").isJsonNull());
    assertTrue(json.get("lastAction").isJsonNull());
    assertEquals("null", JsonParser.parseString(ResponseUtils.getJsonSchema(response)).getAsJsonObject()
        .getAsJsonObject("properties").getAsJsonObject("executionId").get("type").getAsString());
  }

  @Test
  public void testActiveStatusReturnsDecisionAndProgress() throws IOException {
    ControlDecision decision = new ControlDecision(ControlAction.INCREASE, 20971520, ConstraintState.SATISFIED,
                                                   "SLO_HEALTHY", List.of(), 1789020000000L, null);
    RecoveryControlState state = new RecoveryControlState(true, "run-001", 20971520, 104857600, 5242880,
                                                         decision, List.of("PROGRESS_ESTIMATED"));
    JsonObject json = JsonParser.parseString(getStatus(state)).getAsJsonObject();

    assertTrue(json.get("enabled").getAsBoolean());
    assertEquals("run-001", json.get("executionId").getAsString());
    assertEquals(20971520L, json.get("currentThrottleBytesPerSecond").getAsLong());
    assertEquals(104857600L, json.get("remainingBytes").getAsLong());
    assertEquals(5242880L, json.get("progressBytesPerSecond").getAsLong());
    assertEquals("INCREASE", json.get("lastAction").getAsString());
    assertEquals("SATISFIED", json.get("constraintState").getAsString());
    assertEquals("SLO_HEALTHY", json.get("reason").getAsString());
    assertEquals("PROGRESS_ESTIMATED", json.getAsJsonArray("warnings").get(0).getAsString());
    assertEquals(1789020000000L, json.get("decisionTimestampMs").getAsLong());
  }

  private String getStatus(RecoveryControlState state) throws IOException {
    AsyncKafkaCruiseControl cruiseControl = mock(AsyncKafkaCruiseControl.class);
    expect(cruiseControl.config()).andReturn(_config);
    cruiseControl.setUserTaskManagerInExecutor(anyObject());
    expect(cruiseControl.recoveryControlState()).andReturn(state);
    replay(cruiseControl);

    HttpServletRequest request = mock(HttpServletRequest.class);
    CruiseControlRequestContext context = mock(CruiseControlRequestContext.class);
    Map<String, String[]> parameters = Map.of("json", new String[]{"true"});
    expect(request.getSession()).andReturn(null);
    expect(request.getMethod()).andReturn("GET");
    expect(request.getPathInfo()).andReturn(PATH);
    expect(request.getParameterMap()).andReturn(parameters).anyTimes();
    expect(request.getParameter("json")).andReturn("true");
    expect(context.getMethod()).andReturn("GET").anyTimes();
    expect(context.getPathInfo()).andReturn(PATH).anyTimes();
    expect(context.getParameterMap()).andReturn(parameters);
    expect(context.getParameterConfigOverrides()).andReturn(
        Map.of(KAFKA_CRUISE_CONTROL_HTTP_SERVLET_REQUEST_OBJECT_CONFIG, request));
    Capture<String> body = newCapture();
    context.writeResponseToOutputStream(eq(200), eq(true), eq(false), capture(body));
    replay(request, context);

    KafkaCruiseControlRequestHandler handler = new KafkaCruiseControlRequestHandler(cruiseControl, new MetricRegistry());
    try {
      handler.doGetOrPost(context);
      assertTrue(handler.cruiseControlEndPoints().getAllUserTasks().isEmpty());
      verify(cruiseControl, request, context);
      return body.getValue();
    } finally {
      handler.destroy();
    }
  }
}
