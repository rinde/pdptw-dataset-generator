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

import static java.util.Arrays.asList;

import org.junit.Test;

/**
 * @author Rinde van Lon
 *
 */
public class DatasetGeneratorTest {

  @Test
  public void test() {

    System.out.println(DatasetGenerator.builder().setDynamismLevels(
      asList(.10, .12)).dynamismLevels);

    DatasetGenerator.builder().setDynamismLevels(asList(.1, .11));

  }

}
