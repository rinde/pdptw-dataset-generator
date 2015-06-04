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

import java.util.concurrent.Callable;

import javax.annotation.Nullable;

import org.apache.commons.math3.random.MersenneTwister;
import org.apache.commons.math3.random.RandomGenerator;
import org.apache.commons.math3.stat.descriptive.StatisticalSummary;

import com.github.rinde.rinsim.pdptw.common.AddParcelEvent;
import com.github.rinde.rinsim.scenario.Scenario;
import com.github.rinde.rinsim.scenario.generator.ScenarioGenerator;
import com.github.rinde.rinsim.scenario.measure.Metrics;
import com.google.auto.value.AutoValue;

@AutoValue
abstract class ScenarioCreator implements Callable<GeneratedScenario> {

  public abstract long getId();

  public abstract GeneratorSettings getSettings();

  public abstract ScenarioGenerator getGenerator();

  @Nullable
  @Override
  public GeneratedScenario call() throws Exception {
    final RandomGenerator rng = new MersenneTwister(getSettings().getSeed());
    final Scenario scen = getGenerator().generate(rng,
      Long.toString(getId()));

    Metrics.checkTimeWindowStrictness(scen);

    // check that urgency matches expected urgency
    final StatisticalSummary urgency = Metrics.measureUrgency(scen);
    final long expectedUrgency = getSettings().getUrgency();// * 60000L;
    if (!(Math.abs(urgency.getMean() - expectedUrgency) < 0.01
    && urgency.getStandardDeviation() < 0.01)) {
      System.out.println("urgency fail");
      return null;
    }

    // check num orders
    final int numParcels = Metrics.getEventTypeCounts(scen).count(
      AddParcelEvent.class);
    if (numParcels != getSettings().getNumOrders()) {
      System.out.println("orders fail");
      return null;
    }

    // check if dynamism fits in a bin
    final double dynamism = Metrics.measureDynamism(scen,
      getSettings().getOfficeHours());
    @Nullable
    final Double dynamismBin = getSettings().getDynamismRangeCenters().get(
      dynamism);
    if (dynamismBin == null) {
      if (getId() == 44L || getId() == 52L || getId() == 56L) {
        System.out.println(getId() + " dynamism fail " + dynamism + " "
          + getSettings().getDynamismRangeCenters());
      }
      return null;
    }
    return GeneratedScenario.create(scen, getSettings(), getId(), dynamismBin,
      dynamism);
  }

  static ScenarioCreator create(long id, GeneratorSettings set,
    ScenarioGenerator gen) {
    return new AutoValue_ScenarioCreator(id, set, gen);
  }
}
