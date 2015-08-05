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
package com.github.rinde.vanlon15.dataset;

import static java.util.Arrays.asList;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.github.rinde.rinsim.central.RandomSolver;
import com.github.rinde.rinsim.central.rt.RtCentral;
import com.github.rinde.rinsim.experiment.CommandLineProgress;
import com.github.rinde.rinsim.experiment.Experiment;
import com.github.rinde.rinsim.experiment.Experiment.SimulationResult;
import com.github.rinde.rinsim.experiment.ExperimentResults;
import com.github.rinde.rinsim.experiment.MASConfiguration;
import com.github.rinde.rinsim.io.FileProvider;
import com.github.rinde.rinsim.pdptw.common.AddVehicleEvent;
import com.github.rinde.rinsim.scenario.ScenarioConverters;
import com.github.rinde.rinsim.scenario.ScenarioIO;
import com.github.rinde.rinsim.scenario.gendreau06.Gendreau06ObjectiveFunction;
import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.io.Files;

/**
 * @author Rinde van Lon
 *
 */
public class Exp {
  static final Gendreau06ObjectiveFunction SUM = Gendreau06ObjectiveFunction
    .instance();

  static final String DATASET = "files/dataset/";
  static final String RESULTS = "files/results/";

  public static void main(String[] args) {

    final long time = System.currentTimeMillis();
    final Experiment.Builder experimentBuilder = Experiment
      .build(SUM)
      .computeLocal()
      .withRandomSeed(123)
      .repeat(1)
      .setScenarioReader(
        ScenarioIO.readerAdapter(ScenarioConverters.toRealtime()))
      .addScenarios(FileProvider.builder()
        .add(Paths.get(DATASET))
        .filter("glob:**-[0].scen"))
      .addResultListener(new CommandLineProgress(System.out))
      .addConfiguration(
        RtCentral.solverConfigurationAdapt(RandomSolver.supplier(), ""))

    // .addConfiguration(
    // Central.solverConfiguration(RandomSolver.supplier(), "Random"))
    // .addConfiguration(Central.solverConfiguration(
    // CheapestInsertionHeuristic.supplier(SUM), "-CheapInsert"))
    // .addConfiguration(
    // MASConfiguration.pdptwBuilder()
    // .setName("GradientFieldConfiguration")
    // .addEventHandler(AddVehicleEvent.class,
    // GradientFieldExample.VehicleHandler.INSTANCE)
    // .addModel(GradientModel.builder())
    // .build()
    // )
    // .addConfiguration(
    // MASConfiguration
    // .pdptwBuilder()
    // .setName("Auction-R-opt2cih-B-cih")
    // .addEventHandler(
    // AddVehicleEvent.class,
    // new VehicleHandler(
    // SolverRoutePlanner.supplier(
    // Opt2.breadthFirstSupplier(
    // CheapestInsertionHeuristic.supplier(SUM), SUM
    // )
    // ),
    // SolverBidder.supplier(SUM,
    // CheapestInsertionHeuristic.supplier(SUM))
    // )
    // )
    // .addModel(SolverModel.builder())
    // .addModel(AuctionCommModel.builder())
    // .build()
    // )

    // .addConfiguration(
    // Central.solverConfiguration(
    // Opt2.breadthFirstSupplier(CheapestInsertionHeuristic.supplier(SUM),
    // SUM),
    // "opt2cih"))

    // .addConfiguration(
    // MASConfiguration.pdptwBuilder()
    // .setName("Auction-CheapestInsertion")
    // .addEventHandler(AddVehicleEvent.class,
    // new VehicleHandler(
    // SolverRoutePlanner.supplier(
    // CheapestInsertionHeuristic.supplier(SUM)),
    // SolverBidder.supplier(SUM,
    // CheapestInsertionHeuristic.supplier(SUM))
    // ))
    // .addModel(SolverModel.builder())
    // .addModel(AuctionCommModel.builder())
    // .build()
    // )
    // .showGui(View.builder()
    // .with(PlaneRoadModelRenderer.builder())
    // .with(RoadUserRenderer.builder())
    // .with(GradientFieldRenderer.builder())
    // .with(RouteRenderer.builder())
    // // .with(TimeLinePanel.builder())
    // .with(PDPModelRenderer.builder()
    // .withDestinationLines()
    // ).withAutoPlay()
    // )
    ;

    final ExperimentResults results = experimentBuilder.perform();
    final long duration = System.currentTimeMillis() - time;
    System.out.println("Done, computed " + results.getResults().size()
      + " simulations in " + duration / 1000d + "s");

    final Multimap<MASConfiguration, SimulationResult> groupedResults = LinkedHashMultimap
      .create();
    for (final SimulationResult sr : results.sortedResults()) {
      groupedResults.put(sr.getMasConfiguration(), sr);
    }

    for (final MASConfiguration config : groupedResults.keySet()) {
      final Collection<SimulationResult> group = groupedResults.get(config);

      final File configResult = new File(RESULTS + config.getName() + ".csv");
      try {
        Files.createParentDirs(configResult);
      } catch (final IOException e1) {
        throw new IllegalStateException(e1);
      }
      // deletes the file in case it already exists
      configResult.delete();
      try {
        Files
          .append(
            "dynamism,urgency,scale,cost,travel_time,tardiness,over_time,is_valid,scenario_id,random_seed,comp_time,num_vehicles,num_orders\n",
            configResult,
            Charsets.UTF_8);
      } catch (final IOException e1) {
        throw new IllegalStateException(e1);
      }

      for (final SimulationResult sr : group) {
        final String pc = sr.getScenario().getProblemClass().getId();
        final String id = sr.getScenario().getProblemInstanceId();
        final int numVehicles = FluentIterable
          .from(sr.getScenario().getEvents())
          .filter(AddVehicleEvent.class).size();
        try {
          final String scenarioName = Joiner.on("-").join(pc, id);
          final List<String> propsStrings = Files.readLines(new File(
            DATASET + scenarioName + ".properties"),
            Charsets.UTF_8);
          final Map<String, String> properties = Splitter.on("\n")
            .withKeyValueSeparator(" = ")
            .split(Joiner.on("\n").join(propsStrings));

          final double dynamism = Double.parseDouble(properties
            .get("dynamism_bin"));
          final long urgencyMean = Long.parseLong(properties.get("urgency"));
          final double scale = Double.parseDouble(properties.get("scale"));

          final double cost = SUM.computeCost(sr.getStats());
          final double travelTime = SUM.travelTime(sr.getStats());
          final double tardiness = SUM.tardiness(sr.getStats());
          final double overTime = SUM.overTime(sr.getStats());
          final boolean isValidResult = SUM.isValidResult(sr.getStats());
          final long computationTime = sr.getStats().computationTime;

          final long numOrders = Long.parseLong(properties
            .get("AddParcelEvent"));

          final String line = Joiner.on(",")
            .appendTo(new StringBuilder(),
              asList(dynamism, urgencyMean, scale, cost, travelTime,
                tardiness, overTime, isValidResult, scenarioName, sr.getSeed(),
                computationTime, numVehicles, numOrders))
            .append(System.lineSeparator())
            .toString();
          if (!isValidResult) {
            System.err.println("WARNING: FOUND AN INVALID RESULT: ");
            System.err.println(line);
          }
          Files.append(line, configResult, Charsets.UTF_8);
        } catch (final IOException e) {
          throw new IllegalStateException(e);
        }
      }
    }

  }
}
