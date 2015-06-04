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

import java.util.Iterator;

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
      .setDynamismLevels(asList(.1, .3, .5, .7, .9))
      .setUrgencyLevels(asList(5L, 15L, 30L, 45L))
      .setScaleLevels(asList(1d, 2d, 5d, 10d))
      .setNumInstances(50)
      // .setNumThreads(1)
      .build();

    final Dataset<GeneratedScenario> scen = gen.generate();

    final Dataset<GeneratedScenario> scen2 = gen.generate();

    System.out.println(scen.size());
    System.out.println(scen2.size());
    System.out.println(toString(scen));
    System.out.println(toString(scen2));
    System.out.println(toSeedString(scen));
    System.out.println(toSeedString(scen2));

    final Dataset<Scenario> conv1 = DatasetGenerator.convert(scen);
    final Dataset<Scenario> conv2 = DatasetGenerator.convert(scen2);

    final Iterator<GeneratedScenario> it1 = scen.iterator();
    final Iterator<GeneratedScenario> it2 = scen2.iterator();

    while (it1.hasNext()) {

      final GeneratedScenario s1 = it1.next();
      final GeneratedScenario s2 = it2.next();

      assertThat(s1.getScenario().getEvents()).isEqualTo(
        s2.getScenario().getEvents());
      assertThat(s1.getScenario()).isEqualTo(s2.getScenario());

      assertThat(s1).isEqualTo(s2);

    }

    assertThat(scen).isEqualTo(scen2);

    assertThat(conv1).isEqualTo(conv2);

    // System.out.println(Iterators.toString(scen.iterator()));
    //
    // 9, 68, 126, 151, 164, 243, 276, 279, 286, 289, 12, 13, 15, 17, 19, 28,
    // 29, 44, 83, 100,
    // 9, 68, 126, 151, 164, 243, 276, 279, 286, 289, 12, 13, 15, 17, 19, 28,
    // 29, 44, 52, 55,
  }

  static String toString(Dataset<GeneratedScenario> data) {
    final StringBuilder sb = new StringBuilder();
    for (final GeneratedScenario scen : data) {
      sb.append(scen.getId() + ", ");
    }
    return sb.toString();
  }

  static String toSeedString(Dataset<GeneratedScenario> data) {
    final StringBuilder sb = new StringBuilder();
    for (final GeneratedScenario scen : data) {
      sb.append(scen.getSeed() + ", ");
    }
    return sb.toString();
  }
}
