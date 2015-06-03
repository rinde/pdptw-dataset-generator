/*
 * Copyright (C) 2011-2015 Rinde van Lon, iMinds-DistriNet, KU Leuven
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.rinde.dataset;

import com.github.rinde.rinsim.scenario.Scenario;
import com.google.auto.value.AutoValue;

@AutoValue
abstract class VanLonScenario extends Scenario {

  public abstract double getDynamism();

  public abstract long getUrgency();

  public abstract double getScale();

  @Override
  public abstract VanLon15ProblemClass getProblemClass();

  // static VanLonScenario create(Scenario s, double dyn, long urg, double
  // scl) {
  // return new AutoValue_DatasetGenerator_VanLonScenario(
  // s.getEvents(),
  // s.getModelBuilders(),
  // s.getTimeWindow(),
  // s.getStopCondition(),
  // (VanLon15ProblemClass) s.getProblemClass(),
  // s.getProblemInstanceId(),
  // dyn,
  // urg,
  // scl);
  // }
}