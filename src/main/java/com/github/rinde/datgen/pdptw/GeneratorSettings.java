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

import com.github.rinde.datgen.pdptw.DatasetGenerator.TimeSeriesType;
import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableRangeMap;

@AutoValue
abstract class GeneratorSettings {

  public abstract TimeSeriesType getTimeSeriesType();

  public abstract ImmutableRangeMap<Double, Double> getDynamismRangeCenters();

  public abstract int getNumOrders();

  public abstract long getUrgency();

  public abstract double getScale();

  public abstract long getDayLength();

  public abstract long getOfficeHours();

  public abstract ImmutableMap<String, String> getProperties();

  static GeneratorSettings.Builder builder() {
    return new AutoValue_GeneratorSettings.Builder();
  }

  static GeneratorSettings.Builder builder(GeneratorSettings source) {
    return new AutoValue_GeneratorSettings.Builder(source);
  }

  @AutoValue.Builder
  abstract static class Builder {

    abstract Builder setTimeSeriesType(TimeSeriesType type);

    abstract Builder setDynamismRangeCenters(
        ImmutableRangeMap<Double, Double> map);

    abstract Builder setNumOrders(int numOrders);

    abstract Builder setUrgency(long urgency);

    abstract Builder setScale(double scale);

    abstract Builder setDayLength(long length);

    abstract Builder setOfficeHours(long hours);

    abstract Builder setProperties(
        ImmutableMap<String, String> props);

    abstract GeneratorSettings build();

  }
}
