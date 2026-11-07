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

package com.linkedin.kafka.cruisecontrol.servlet.handler.sync;

import com.linkedin.cruisecontrol.http.CruiseControlRequestContext;
import com.linkedin.kafka.cruisecontrol.async.AsyncKafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.servlet.handler.AbstractRequest;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.RecoveryControlParameters;
import com.linkedin.kafka.cruisecontrol.servlet.response.RecoveryControlStatus;
import java.util.Map;

import static com.linkedin.cruisecontrol.common.utils.Utils.validateNotNull;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.RECOVERY_CONTROL_PARAMETER_OBJECT_CONFIG;


/** Returns the current recovery snapshot without allocating a user task. */
public final class RecoveryControlRequest extends AbstractRequest {
  private AsyncKafkaCruiseControl _cruiseControl;
  private RecoveryControlParameters _parameters;

  @Override
  protected RecoveryControlStatus getResponse(CruiseControlRequestContext requestContext) {
    return new RecoveryControlStatus(_cruiseControl.recoveryControlState());
  }

  @Override
  public RecoveryControlParameters parameters() {
    return _parameters;
  }

  @Override
  public void configure(Map<String, ?> configs) {
    super.configure(configs);
    _cruiseControl = _requestHandler.cruiseControlEndPoints().asyncKafkaCruiseControl();
    _parameters = (RecoveryControlParameters) validateNotNull(configs.get(RECOVERY_CONTROL_PARAMETER_OBJECT_CONFIG),
        "Recovery control parameter configuration is missing from the request.");
  }
}
