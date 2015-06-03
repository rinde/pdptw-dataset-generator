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

import static com.google.common.truth.Truth.assertThat;
import static java.util.Arrays.asList;

import org.junit.Test;

import com.github.rinde.rinsim.scenario.Scenario;

/**
 * @author Rinde van Lon
 *
 */
public class DatasetGeneratorTest {

  @Test
  public void testSetDynamismLevels() {

    boolean fail = false;
    try {
      DatasetGenerator.builder().setDynamismLevels(asList(.1, .11));
    } catch (final IllegalArgumentException e) {
      assertThat(e.getMessage()).containsMatch("too close");
      fail = true;
    }
    assertThat(fail).isTrue();

  }

  @Test
  public void test() {
    final DatasetGenerator gen = DatasetGenerator.builder()
      .setDynamismLevels(asList(.3, .52))
      .setUrgencyLevels(asList(15L))
      .setScaleLevels(asList(1d))
      .setNumInstances(10)
      .build();

    final Dataset<Scenario> scen = gen.generate();

    final Dataset<Scenario> scen2 = gen.generate();

    System.out.println(scen.size());
    System.out.println(scen2.size());

    assertThat(scen).isEqualTo(scen2);

    // System.out.println(Iterators.toString(scen.iterator()));
  }
}
