/*
 * Copyright (C) 2015 Rinde van Lon, iMinds-DistriNet, KU Leuven
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
package com.github.rinde.datgen.pdptw;

import static com.google.common.base.Verify.verifyNotNull;

import javax.annotation.Nullable;

import com.github.rinde.rinsim.scenario.Scenario;
import com.google.auto.value.AutoValue;

@AutoValue
abstract class GeneratedScenario implements Comparable<GeneratedScenario> {

  public abstract Scenario getScenario();

  public abstract GeneratorSettings getSettings();

  public abstract long getId();

  public abstract long getSeed();

  public abstract double getDynamismBin();

  public abstract double getActualDynamism();

  @Override
  public int compareTo(@Nullable GeneratedScenario scen) {
    return Long.compare(getId(), verifyNotNull(scen).getId());
  }

  @Override
  public String toString() {
    return "(" + getId() + "/" + getSeed() + ")";
  }

  static GeneratedScenario create(Scenario s, GeneratorSettings settings,
      long id, long seed, double dynBin, double actDyn) {
    return new AutoValue_GeneratedScenario(s, settings, id, seed, dynBin,
        actDyn);
  }
}
