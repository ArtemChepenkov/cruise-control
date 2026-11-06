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

import org.apache.kafka.common.Configurable;


/** A deterministic policy that evaluates a recovery snapshot without performing I/O. */
public interface ThrottleControlPolicy extends Configurable {
  /**
   * Evaluate a snapshot and determine the next throttle.
   *
   * @param input measurements and settings for this control cycle
   * @return the decision for the input snapshot
   */
  ControlDecision decide(ControlInput input);
}
