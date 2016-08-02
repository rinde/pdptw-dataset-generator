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

import static com.google.common.truth.Truth.assertThat;
import static java.util.Arrays.asList;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.Test;

import com.github.rinde.logistics.pdptw.mas.TruckFactory.DefaultTruckFactory;
import com.github.rinde.logistics.pdptw.mas.comm.AuctionCommModel;
import com.github.rinde.logistics.pdptw.mas.comm.DoubleBid;
import com.github.rinde.logistics.pdptw.mas.comm.SolverBidder;
import com.github.rinde.logistics.pdptw.mas.route.SolverRoutePlanner;
import com.github.rinde.logistics.pdptw.solver.CheapestInsertionHeuristic;
import com.github.rinde.rinsim.central.SolverModel;
import com.github.rinde.rinsim.experiment.Experiment;
import com.github.rinde.rinsim.experiment.MASConfiguration;
import com.github.rinde.rinsim.pdptw.common.AddVehicleEvent;
import com.github.rinde.rinsim.pdptw.common.ObjectiveFunction;
import com.github.rinde.rinsim.pdptw.common.TimeLinePanel;
import com.github.rinde.rinsim.scenario.Scenario;
import com.github.rinde.rinsim.scenario.ScenarioIO;
import com.github.rinde.rinsim.scenario.gendreau06.Gendreau06ObjectiveFunction;
import com.github.rinde.rinsim.ui.View;
import com.github.rinde.rinsim.ui.renderers.PDPModelRenderer;
import com.github.rinde.rinsim.ui.renderers.PlaneRoadModelRenderer;
import com.github.rinde.rinsim.ui.renderers.RoadUserRenderer;
import com.github.rinde.rinsim.util.TimeWindow;

/**
 * @author Rinde van Lon
 *
 */
public class DatasetGeneratorTest {

  /**
   * Tests that illegal dynamism levels are handled.
   */
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

  /**
   * Tests that two invocations produce the same dataset.
   */
  @Test
  public void consistencyTest() {
    final DatasetGenerator gen = DatasetGenerator.builder()
      .setDynamismLevels(asList(.1, .5, .6, .7))
      .setUrgencyLevels(asList(15L))
      .setScaleLevels(asList(1d))
      .setNumInstances(1)
      .setDatasetDir("files/test/dataset/")
      // .setNumThreads(1)
      .build();

    final Dataset<GeneratedScenario> scen = gen.doGenerate();
    final Dataset<GeneratedScenario> scen2 = gen.doGenerate();

    final Dataset<Scenario> conv1 = gen.convert(scen);
    final Dataset<Scenario> conv2 = gen.convert(scen2);

    assertThat(scen).isEqualTo(scen2);
    assertThat(conv1).isEqualTo(conv2);
  }

  // @Test
  public void test2() {
    try (final DirectoryStream<Path> directoryStream = Files
      .newDirectoryStream(Paths.get("files/test/dataset/"), "*.scen")) {

      for (final Path path : directoryStream) {
        System.out.println(path);
        run(path.toString());
      }
    } catch (final IOException ex) {
      ex.printStackTrace();
    }
  }

  @Test
  public void testScenarioLength() {
    final DatasetGenerator genDefault = DatasetGenerator.builder()
      .build();
    final DatasetGenerator gen2 = DatasetGenerator.builder()
      .setScenarioLength(2)
      .build();
    final DatasetGenerator gen5 = DatasetGenerator.builder()
      .setScenarioLength(5)
      .build();

    assertThat(genDefault.generate().next().getTimeWindow())
      .isEqualTo(TimeWindow.create(0, 4 * 60 * 60 * 1000L));
    assertThat(gen2.generate().next().getTimeWindow())
      .isEqualTo(TimeWindow.create(0, 2 * 60 * 60 * 1000L));
    assertThat(gen5.generate().next().getTimeWindow())
      .isEqualTo(TimeWindow.create(0, 5 * 60 * 60 * 1000L));
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

  private static void run(final String fileName) {
    final Scenario scen;
    try {
      scen = ScenarioIO.read(new File(fileName).toPath());
    } catch (final IOException e) {
      throw new IllegalStateException(e);
    }
    final ObjectiveFunction objFunc = Gendreau06ObjectiveFunction.instance();
    Experiment.builder()
      .addScenario(scen)
      .addConfiguration(MASConfiguration.pdptwBuilder()
        .addEventHandler(AddVehicleEvent.class,
          DefaultTruckFactory.builder()
            .setRoutePlanner(SolverRoutePlanner.supplier(
              CheapestInsertionHeuristic.supplier(objFunc)))
            .setCommunicator(SolverBidder.supplier(objFunc,
              CheapestInsertionHeuristic.supplier(objFunc)))
            .build())
        .addModel(AuctionCommModel.builder(DoubleBid.class))
        .addModel(SolverModel.builder())
        .build())
      .withThreads(1)
      .showGui(
        // schema.add(Vehicle.class, SWT.COLOR_RED);
        // schema.add(Depot.class, SWT.COLOR_CYAN);
        // schema.add(Parcel.class, SWT.COLOR_BLUE);
        View.builder()
          .with(PlaneRoadModelRenderer.builder())
          .with(RoadUserRenderer.builder()
    // .withColorAssociation(Vehicle.class, SWT.COLOR_RED)
    )
          .with(PDPModelRenderer.builder())
          .with(TimeLinePanel.builder())
          .withTitleAppendix(fileName)
          .withAutoPlay()
          .withAutoClose()
          .withSpeedUp(80))
      .perform();
  }
}
