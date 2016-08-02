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

import static com.github.rinde.rinsim.util.StochasticSuppliers.constant;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verifyNotNull;
import static java.util.Arrays.asList;

import java.io.File;
import java.io.IOException;
import java.math.RoundingMode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.Nullable;
import javax.measure.unit.NonSI;
import javax.measure.unit.SI;

import org.apache.commons.math3.random.MersenneTwister;
import org.apache.commons.math3.random.RandomGenerator;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import com.github.rinde.rinsim.core.model.pdp.DefaultPDPModel;
import com.github.rinde.rinsim.core.model.pdp.Parcel;
import com.github.rinde.rinsim.core.model.pdp.TimeWindowPolicy.TimeWindowPolicies;
import com.github.rinde.rinsim.core.model.road.RoadModelBuilders;
import com.github.rinde.rinsim.core.model.time.RealtimeClockController.ClockMode;
import com.github.rinde.rinsim.core.model.time.TimeModel;
import com.github.rinde.rinsim.geom.Point;
import com.github.rinde.rinsim.pdptw.common.PDPRoadModel;
import com.github.rinde.rinsim.pdptw.common.StatsStopConditions;
import com.github.rinde.rinsim.scenario.Scenario;
import com.github.rinde.rinsim.scenario.Scenario.ProblemClass;
import com.github.rinde.rinsim.scenario.ScenarioIO;
import com.github.rinde.rinsim.scenario.StopConditions;
import com.github.rinde.rinsim.scenario.generator.Depots;
import com.github.rinde.rinsim.scenario.generator.IntensityFunctions;
import com.github.rinde.rinsim.scenario.generator.Locations;
import com.github.rinde.rinsim.scenario.generator.Locations.LocationGenerator;
import com.github.rinde.rinsim.scenario.generator.Parcels;
import com.github.rinde.rinsim.scenario.generator.ScenarioGenerator;
import com.github.rinde.rinsim.scenario.generator.ScenarioGenerator.TravelTimes;
import com.github.rinde.rinsim.scenario.generator.TimeSeries;
import com.github.rinde.rinsim.scenario.generator.TimeSeries.TimeSeriesGenerator;
import com.github.rinde.rinsim.scenario.generator.TimeWindows.TimeWindowGenerator;
import com.github.rinde.rinsim.scenario.generator.Vehicles;
import com.github.rinde.rinsim.scenario.measure.Metrics;
import com.github.rinde.rinsim.scenario.measure.MetricsIO;
import com.github.rinde.rinsim.scenario.vanlon15.VanLon15ProblemClass;
import com.github.rinde.rinsim.util.StochasticSupplier;
import com.github.rinde.rinsim.util.StochasticSuppliers;
import com.github.rinde.rinsim.util.TimeWindow;
import com.google.auto.value.AutoValue;
import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.ImmutableRangeMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Multiset;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.RangeSet;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.TreeRangeMap;
import com.google.common.collect.TreeRangeSet;
import com.google.common.math.DoubleMath;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

/**
 * Generator for datasets consisting of dynamic PDPTW scenarios with varying
 * levels of dynamism, urgency and scale. Instances can be obtained and
 * configured via {@link #builder()}. The total number of instances that is
 * generated is the product of the following parameters:
 * <ul>
 * <li>number of dynamism levels</li>
 * <li>number of urgency levels</li>
 * <li>number of scale levels</li>
 * <li>number of instances</li>
 * </ul>
 * @author Rinde van Lon
 */
public final class DatasetGenerator {
  private static final long THREAD_SLEEP_DURATION = 100L;
  private static final long MS_IN_MIN = 60000L;
  private static final long MS_IN_H = 60 * MS_IN_MIN;
  private static final long TICK_SIZE = 1000L;
  private static final double VEHICLE_SPEED_KMH = 50d;

  // n x n (km)
  private static final double AREA_WIDTH = 10;
  private static final int ORDERS_P_HOUR = 30;

  private static final long HALF_DIAG_TT = 509117L;
  private static final long ONE_AND_HALF_DIAG_TT = 1527351L;
  private static final long TWO_DIAG_TT = 2036468L;

  private static final long PICKUP_DURATION = 5 * 60 * 1000L;
  private static final long DELIVERY_DURATION = 5 * 60 * 1000L;

  private static final int VEHICLES_PER_SCALE = 10;

  private static final long INTENSITY_PERIOD = 60 * 60 * 1000L;

  // These parameters influence the dynamism selection settings
  private static final double DYN_BANDWIDTH = 0.01;
  // number of digits
  private static final double DYN_PRECISION = 2;

  private static final String TIME_SERIES = "time_series";

  final Builder builder;

  final int numOrdersPerScale;

  DatasetGenerator(Builder b) {
    builder = b;

    numOrdersPerScale = (int) (ORDERS_P_HOUR * b.scenarioLengthHours);
  }

  /**
   * Generates the dataset. When generation is done all files are written to
   * disk.
   * @return An iterator for all generated scenarios.
   */
  public Iterator<Scenario> generate() {
    return convert(doGenerate()).iterator();
  }

  Dataset<GeneratedScenario> doGenerate() {

    final ListeningExecutorService service = MoreExecutors
      .listeningDecorator(Executors.newFixedThreadPool(builder.numThreads));
    final Dataset<GeneratedScenario> dataset = Dataset.naturalOrder();

    final List<ScenarioCreator> jobs = new ArrayList<>();

    final RandomGenerator rng = new MersenneTwister(builder.randomSeed);
    final Map<GeneratorSettings, IdSeedGenerator> rngMap =
      new LinkedHashMap<>();

    for (final Long urgency : builder.urgencyLevels) {
      for (final Double scale : builder.scaleLevels) {
        for (final Entry<TimeSeriesType, Collection<Range<Double>>> dynLevel : builder.dynamismLevels
          .asMap().entrySet()) {

          final int reps = builder.numInstances * dynLevel.getValue().size();

          final long urg = urgency * 60 * 1000L;
          // The office hours is the period in which new orders are accepted,
          // it is defined as [0,officeHoursLength).
          final long officeHoursLength;
          if (urg < HALF_DIAG_TT) {
            officeHoursLength = builder.scenarioLengthMs
              - TWO_DIAG_TT
              - PICKUP_DURATION
              - DELIVERY_DURATION;
          } else {
            officeHoursLength = builder.scenarioLengthMs
              - urg
              - ONE_AND_HALF_DIAG_TT
              - PICKUP_DURATION - DELIVERY_DURATION;
          }

          final int numOrders = DoubleMath.roundToInt(scale * numOrdersPerScale,
            RoundingMode.UNNECESSARY);

          final ImmutableMap.Builder<String, String> props = ImmutableMap
            .builder();

          props.put("expected_num_orders", Integer.toString(numOrders));
          props.put("pickup_duration", Long.toString(PICKUP_DURATION));
          props.put("delivery_duration", Long.toString(DELIVERY_DURATION));
          props.put("width_height",
            String.format("%1.1fx%1.1f", AREA_WIDTH, AREA_WIDTH));

          // TODO store this in TimeSeriesType?
          final RangeSet<Double> rset = TreeRangeSet.create();
          for (final Range<Double> r : dynLevel.getValue()) {
            rset.add(r);
          }

          createTimeSeriesGenerator(dynLevel.getKey(), officeHoursLength,
            numOrders, numOrdersPerScale, props);

          final GeneratorSettings set = GeneratorSettings
            .builder()
            .setDayLength(builder.scenarioLengthMs)
            .setOfficeHours(officeHoursLength)
            .setTimeSeriesType(dynLevel.getKey())
            .setDynamismRangeCenters(
              builder.dynamismRangeMap.subRangeMap(rset.span()))
            .setUrgency(urg)
            .setScale(scale)
            .setNumOrders(numOrders)
            .setProperties(props.build())
            .build();

          final IdSeedGenerator isg = new IdSeedGenerator(rng.nextLong());
          rngMap.put(set, isg);

          for (int i = 0; i < reps; i++) {
            final LocationGenerator lg = Locations.builder()
              .min(0d)
              .max(AREA_WIDTH)
              .buildUniform();

            final TimeSeriesGenerator tsg2 = createTimeSeriesGenerator(
              dynLevel.getKey(), officeHoursLength, numOrders,
              numOrdersPerScale, ImmutableMap.<String, String>builder());
            final ScenarioGenerator gen = createGenerator(
              builder.scenarioLengthMs, urg, scale, tsg2, lg,
              numOrdersPerScale);

            jobs.add(ScenarioCreator.create(isg.next(), set, gen));
          }
        }
      }
    }

    final AtomicLong currentJobs = new AtomicLong(0L);
    final AtomicLong datasetSize = new AtomicLong(0L);

    for (final ScenarioCreator job : jobs) {
      submitJob(currentJobs, service, job, builder.numInstances, dataset,
        rngMap, datasetSize);
    }

    final long targetSize = builder.numInstances
      * builder.dynamismLevels.values().size() * builder.scaleLevels.size()
      * builder.urgencyLevels.size();
    while (datasetSize.get() < targetSize || dataset.size() < targetSize) {
      try {
        Thread.sleep(THREAD_SLEEP_DURATION);
      } catch (final InterruptedException e) {
        throw new IllegalStateException(e);
      }
    }

    service.shutdown();
    try {
      service.awaitTermination(1L, TimeUnit.HOURS);
    } catch (final InterruptedException e) {
      throw new IllegalStateException(e);
    }

    return dataset;
  }

  Dataset<Scenario> convert(Dataset<GeneratedScenario> input) {
    final Dataset<Scenario> data = Dataset
      .orderedBy(ScenarioComparator.INSTANCE);
    for (final GeneratedScenario gs : input) {
      final GeneratorSettings settings = gs.getSettings();

      final double dyn = gs.getDynamismBin();
      final long urg = settings.getUrgency() / MS_IN_MIN;
      final double scl = settings.getScale();

      final int cur = data.get(dyn, urg, scl).size();
      final ProblemClass pc = VanLon15ProblemClass.create(dyn, urg, scl);
      final Scenario finalScenario = Scenario.builder(pc)
        .copyProperties(gs.getScenario())
        .problemClass(pc)
        .instanceId(Integer.toString(cur))
        .build();

      if (builder.datasetDir.getNameCount() > 0) {
        writeScenario(finalScenario, gs.getActualDynamism(), gs.getSeed(),
          settings);
      }

      data.put(dyn, urg, scl, finalScenario);
    }
    return data;
  }

  void writeScenario(Scenario s, double actualDyn, long seed,
      GeneratorSettings set) {

    final String instanceId = s.getProblemInstanceId();
    final Path filePath = Paths.get(builder.datasetDir.toString(),
      s.getProblemClass().getId() + "-" + instanceId);

    try {
      Files.createDirectories(builder.datasetDir);

      writePropertiesFile(s, set, actualDyn, seed, filePath.toString());
      MetricsIO.writeLocationList(Metrics.getServicePoints(s),
        new File(filePath.toString() + ".points"));
      MetricsIO.writeTimes(s.getTimeWindow().end(),
        Metrics.getArrivalTimes(s),
        new File(filePath.toString() + ".times"));

      ScenarioIO.write(s,
        new File(filePath.toString() + ".scen").toPath());

    } catch (final IOException e) {
      throw new IllegalStateException(e);
    }
  }

  static void writePropertiesFile(Scenario scen,
      GeneratorSettings settings, double actualDyn, long seed,
      String fileName) {
    final DateTimeFormatter formatter = ISODateTimeFormat
      .dateHourMinuteSecondMillis();

    final VanLon15ProblemClass pc = (VanLon15ProblemClass) scen
      .getProblemClass();
    final ImmutableMap.Builder<String, Object> properties = ImmutableMap
      .<String, Object>builder()
      .put("problem_class", pc.getId())
      .put("id", scen.getProblemInstanceId())
      .put("dynamism_bin", pc.getDynamism())
      .put("dynamism_actual", actualDyn)
      .put("urgency", pc.getUrgency())
      .put("scale", pc.getScale())
      .put("random_seed", seed)
      .put("creation_date", formatter.print(System.currentTimeMillis()))
      .put("creator", System.getProperty("user.name"))
      .put("day_length", settings.getDayLength())
      .put("office_opening_hours", settings.getOfficeHours());

    properties.putAll(settings.getProperties());

    final ImmutableMultiset<Class<?>> eventTypes = Metrics
      .getEventTypeCounts(scen);
    for (final Multiset.Entry<Class<?>> en : eventTypes.entrySet()) {
      properties.put(en.getElement().getSimpleName(), en.getCount());
    }

    try {
      Files.write(
        Paths.get(fileName + ".properties"),
        asList(Joiner.on("\n").withKeyValueSeparator(" = ")
          .join(properties.build())),
        Charsets.UTF_8);
    } catch (final IOException e) {
      throw new IllegalStateException(e);
    }
  }

  static void submitJob(final AtomicLong currentJobs,
      final ListeningExecutorService service,
      final ScenarioCreator job,
      final int numInstances,
      final Dataset<GeneratedScenario> dataset,
      final Map<GeneratorSettings, IdSeedGenerator> rngMap,
      final AtomicLong datasetSize) {

    if (service.isShutdown()) {
      return;
    }
    currentJobs.getAndIncrement();
    final ListenableFuture<GeneratedScenario> future = service.submit(job);
    Futures.addCallback(future, new FutureCallback<GeneratedScenario>() {
      @Override
      public void onSuccess(@Nullable GeneratedScenario result) {
        currentJobs.decrementAndGet();
        if (result == null) {
          final ScenarioCreator newJob = ScenarioCreator.create(
            rngMap.get(job.getSettings()).next(),
            job.getSettings(),
            job.getGenerator());

          submitJob(currentJobs, service, newJob, numInstances, dataset,
            rngMap, datasetSize);
          return;
        }
        final GeneratedScenario res = verifyNotNull(result);
        if (dataset.get(res.getDynamismBin(),
          res.getSettings().getUrgency(),
          res.getSettings().getScale()).size() < numInstances) {

          datasetSize.getAndIncrement();
          dataset.put(res.getDynamismBin(),
            res.getSettings().getUrgency(),
            res.getSettings().getScale(),
            res);
        } else {
          // TODO check if this job should be respawned by seeing if it uses the
          // correct TSG

          // TODO respawn more tasks if currentJobs < numThreads
          final Collection<Double> dynamismLevels = job.getSettings()
            .getDynamismRangeCenters().asMapOfRanges().values();

          boolean needMore = false;
          for (final Double d : dynamismLevels) {
            if (dataset.get(d, res.getSettings().getUrgency(),
              res.getSettings().getScale()).size() < numInstances) {
              needMore = true;
              break;
            }
          }

          if (needMore) {
            // respawn job

            final ScenarioCreator newJob = ScenarioCreator.create(
              rngMap.get(job.getSettings()).next(), job.getSettings(),
              job.getGenerator());

            if (!service.isShutdown()) {
              submitJob(currentJobs, service, newJob, numInstances,
                dataset, rngMap, datasetSize);
            }
          }
        }
      }

      @Override
      public void onFailure(Throwable t) {
        throw new IllegalStateException(t);
      }
    }, MoreExecutors.directExecutor());
  }

  static String normal(double m, double std) {
    return normal2("N", m, std);
  }

  static String uniform(double m, double std) {
    return normal2("U", m, std);
  }

  static String normal2(String letter, double m, double std) {
    return letter + "(" + m + "," + std + ")";
  }

  static TimeSeriesGenerator createTimeSeriesGenerator(TimeSeriesType type,
      long officeHoursLength, int numOrders, int numOrdersPerScale,
      ImmutableMap.Builder<String, String> props) {

    final String tString = "t > 0";

    final double numPeriods = officeHoursLength / (double) INTENSITY_PERIOD;
    if (type == TimeSeriesType.POISSON_SINE) {
      props.put(TIME_SERIES, "non-homogenous (sine) Poisson process");
      props.put("time_series.sine.period", Long.toString(INTENSITY_PERIOD));
      props.put("time_series.sine.num_periods", Double.toString(numPeriods));

      final double heightL = -.99;
      final double heightU = 3d;
      props.put("time_series.sine.height", uniform(heightL, heightU));
      props.put("time_series.sine.phase_shitft", uniform(0, INTENSITY_PERIOD));

      final TimeSeriesGenerator sineTsg = TimeSeries.nonHomogenousPoisson(
        officeHoursLength,
        IntensityFunctions
          .sineIntensity()
          .area(numOrders / numPeriods)
          .period(INTENSITY_PERIOD)
          .height(StochasticSuppliers.uniformDouble(heightL, heightU))
          .phaseShift(
            StochasticSuppliers.uniformDouble(0, INTENSITY_PERIOD))
          .buildStochasticSupplier());
      return sineTsg;
    } else if (type == TimeSeriesType.POISSON_HOMOGENOUS) {
      props.put(TIME_SERIES, "homogenous Poisson process");
      props.put("time_series.intensity",
        Double.toString((double) numOrdersPerScale / officeHoursLength));
      return TimeSeries.homogenousPoisson(officeHoursLength, numOrders);
    } else if (type == TimeSeriesType.NORMAL) {
      props.put(TIME_SERIES, "normal distribution");
      final double mean = officeHoursLength / (double) numOrders;
      final double sd = 2.4 * 60 * 1000;
      props.put("time_series.normal", normal(mean, sd));
      props.put("time_series.normal.truncated", tString);
      return TimeSeries.normal(officeHoursLength, numOrders, sd);
    } else if (type == TimeSeriesType.UNIFORM) {
      props.put(TIME_SERIES, "uniform distribution");

      final double mean = 1 * 60 * 1000;
      final double std = 1 * 60 * 1000;
      final double lb = 0;
      final double ub = 15d * 60 * 1000;

      props.put("time_series.uniform.mean",
        Double.toString(officeHoursLength / (double) numOrders));
      props.put("time_series.uniform.truncated", tString);
      props.put("time_series.uniform.max_dev", normal(mean, std));
      props.put("time_series.uniform.max_dev.lower_bound", Double.toString(lb));
      props.put("time_series.uniform.max_dev.upper_bound", Double.toString(ub));
      props.put("time_series.uniform.max_dev.out_of_bounds_strategy", "redraw");
      final StochasticSupplier<Double> maxDeviation = StochasticSuppliers
        .normal()
        .mean(mean)
        .std(std)
        .lowerBound(lb)
        .upperBound(ub)
        .redrawWhenOutOfBounds()
        .buildDouble();
      return TimeSeries.uniform(officeHoursLength, numOrders, maxDeviation);
    }
    throw new IllegalStateException();
  }

  static ScenarioGenerator createGenerator(long scenarioLength,
      long urgency, double scale, TimeSeriesGenerator tsg,
      LocationGenerator lg, int numOrdersPerScale) {
    return ScenarioGenerator.builder()

      // global
      .addModel(TimeModel.builder()
        .withRealTime()
        .withStartInClockMode(ClockMode.SIMULATED)
        .withTickLength(TICK_SIZE)
        .withTimeUnit(SI.MILLI(SI.SECOND)))
      .scenarioLength(scenarioLength)
      .setStopCondition(StopConditions.and(
        StatsStopConditions.vehiclesDoneAndBackAtDepot(),
        StatsStopConditions.timeOutEvent()))
      // parcels
      .parcels(
        Parcels.builder()
          .announceTimes(
            TimeSeries.filter(tsg, TimeSeries.numEventsPredicate(
              DoubleMath.roundToInt(numOrdersPerScale * scale,
                RoundingMode.UNNECESSARY))))
          .pickupDurations(constant(PICKUP_DURATION))
          .deliveryDurations(constant(DELIVERY_DURATION))
          .neededCapacities(constant(0))
          .locations(lg)
          .timeWindows(new CustomTimeWindowGenerator(urgency))
          .build())

      // vehicles
      .vehicles(
        Vehicles
          .builder()
          .capacities(constant(1))
          .centeredStartPositions()
          .creationTimes(constant(-1L))
          .numberOfVehicles(
            constant(DoubleMath.roundToInt(VEHICLES_PER_SCALE * scale,
              RoundingMode.UNNECESSARY)))
          .speeds(constant(VEHICLE_SPEED_KMH))
          .timeWindowsAsScenario()
          .build())

      // depots
      .depots(Depots.singleCenteredDepot())

      // models
      .addModel(
        PDPRoadModel.builder(
          RoadModelBuilders.plane()
            .withMaxSpeed(VEHICLE_SPEED_KMH)
            .withSpeedUnit(NonSI.KILOMETERS_PER_HOUR)
            .withDistanceUnit(SI.KILOMETER))
          .withAllowVehicleDiversion(true))
      .addModel(
        DefaultPDPModel.builder()
          .withTimeWindowPolicy(TimeWindowPolicies.TARDY_ALLOWED))
      .build();
  }

  /**
   * @return A new {@link Builder} for {@link DatasetGenerator} instances.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Builder for {@link DatasetGenerator} instances.
   * @author Rinde van Lon
   */
  public static class Builder {
    static final double DYNAMISM_T1 = 0.475;
    static final double DYNAMISM_T2 = 0.575;
    static final double DYNAMISM_T3 = 0.675;

    static final double DEFAULT_DYN = .5;
    static final long DEFAULT_URG = 20L;
    static final double DEFAULT_SCL = 1d;
    static final int DEFAULT_NUM_INSTANCES = 1;
    static final long DEFAULT_SCENARIO_HOURS = 4L;
    static final long DEFAULT_SCENARIO_LENGTH =
      DEFAULT_SCENARIO_HOURS * MS_IN_H;

    static final ImmutableRangeMap<Double, TimeSeriesType> DYNAMISM_MAP =
      ImmutableRangeMap.<Double, TimeSeriesType>builder()
        .put(Range.closedOpen(0.000, DYNAMISM_T1),
          TimeSeriesType.POISSON_SINE)
        .put(Range.closedOpen(DYNAMISM_T1, DYNAMISM_T2),
          TimeSeriesType.POISSON_HOMOGENOUS)
        .put(Range.closedOpen(DYNAMISM_T2, DYNAMISM_T3),
          TimeSeriesType.NORMAL)
        .put(Range.closed(DYNAMISM_T3, 1.000), TimeSeriesType.UNIFORM)
        .build();

    long randomSeed;
    ImmutableSet<Double> scaleLevels;
    ImmutableSetMultimap<TimeSeriesType, Range<Double>> dynamismLevels;
    ImmutableRangeMap<Double, Double> dynamismRangeMap;
    ImmutableSet<Long> urgencyLevels;
    int numInstances;
    int numThreads;
    Path datasetDir;
    long scenarioLengthHours;
    long scenarioLengthMs;

    Builder() {
      randomSeed = 0L;
      scaleLevels = ImmutableSet.of(DEFAULT_SCL);
      dynamismLevels = ImmutableSetMultimap.of(
        TimeSeriesType.POISSON_HOMOGENOUS, createDynRange(DEFAULT_DYN));
      dynamismRangeMap =
        ImmutableRangeMap.of(createDynRange(DEFAULT_DYN), DEFAULT_DYN);
      urgencyLevels = ImmutableSet.of(DEFAULT_URG);
      numInstances = DEFAULT_NUM_INSTANCES;
      numThreads = Runtime.getRuntime().availableProcessors();
      datasetDir = Paths.get("/");
      scenarioLengthHours = DEFAULT_SCENARIO_HOURS;
      scenarioLengthMs = DEFAULT_SCENARIO_LENGTH;
    }

    /**
     * Sets the random seed to use.
     * @param seed The seed to use.
     * @return This, as per the builder pattern.
     */
    public Builder setRandomSeed(long seed) {
      randomSeed = seed;
      return this;
    }

    /**
     * Sets the scenario length in hours.
     * @param hours The length of the scenario.
     * @return This, as per the builder pattern.
     */
    public Builder setScenarioLength(long hours) {
      scenarioLengthHours = hours;
      scenarioLengthMs = hours * MS_IN_H;
      return this;
    }

    /**
     * Sets the dynamism levels.
     * @param levels At least one level must be given. The default level is
     *          <code>.5</code>.
     * @return This, as per the builder pattern.
     */
    public Builder setDynamismLevels(Iterable<Double> levels) {
      checkArgument(Iterables.size(levels) > 0);
      final RangeSet<Double> rangeSet = TreeRangeSet.create();
      final Set<Range<Double>> dynamismLevelsB = new LinkedHashSet<>();
      final RangeMap<Double, Double> map = TreeRangeMap.create();
      for (final Double d : levels) {
        checkArgument(d >= 0d && d <= 1d);
        final Range<Double> newRange = createDynRange(d);
        checkArgument(
          rangeSet.subRangeSet(newRange).isEmpty(),
          "Can not add dynamism level %s, it is too close to another level.",
          d);
        rangeSet.add(newRange);
        dynamismLevelsB.add(newRange);
        map.put(newRange, d);
      }

      final SetMultimap<TimeSeriesType, Range<Double>> timeSeriesTypes =
        LinkedHashMultimap
          .<TimeSeriesType, Range<Double>>create();

      for (final Range<Double> r : dynamismLevelsB) {
        checkArgument(DYNAMISM_MAP.get(r.lowerEndpoint()) != null);
        checkArgument(DYNAMISM_MAP.get(r.lowerEndpoint()) == DYNAMISM_MAP.get(r
          .upperEndpoint()));

        timeSeriesTypes.put(DYNAMISM_MAP.get(r.lowerEndpoint()), r);
      }
      dynamismLevels = ImmutableSetMultimap.copyOf(timeSeriesTypes);
      dynamismRangeMap = ImmutableRangeMap.copyOf(map);
      return this;
    }

    /**
     * Sets the levels of urgency, urgency is expressed in minutes.
     * @param levels At least one level must be specified, each level must be a
     *          positive number.
     * @return This, as per the builder pattern.
     */
    public Builder setUrgencyLevels(Iterable<Long> levels) {
      checkArgument(Iterables.size(levels) > 0);
      for (final Long l : levels) {
        checkArgument(l > 0);
      }
      urgencyLevels = ImmutableSet.copyOf(levels);
      return this;
    }

    /**
     * Sets the scale levels.
     * @param levels At least one level must be given, each level must be a
     *          positive number. The default level is <code>1</code>.
     * @return This, as per the builder pattern.
     */
    public Builder setScaleLevels(Iterable<Double> levels) {
      checkArgument(Iterables.size(levels) > 0);
      for (final Double d : levels) {
        checkArgument(d > 0d);
      }
      scaleLevels = ImmutableSet.copyOf(levels);
      return this;
    }

    /**
     * Sets the number of instances that should be generated for each
     * combination of dynamism, urgency and scale.
     * @param num The number of instances, must be a positive number.
     * @return This, as per the builder pattern.
     */
    public Builder setNumInstances(int num) {
      checkArgument(num > 0);
      numInstances = num;
      return this;
    }

    /**
     * Sets the number of threads to use when generating the scenarios. By
     * default this is the number of processors as returned by
     * {@link Runtime#availableProcessors()}.
     * @param i The number of threads to use.
     * @return This, as per the builder pattern.
     */
    public Builder setNumThreads(int i) {
      numThreads = i;
      return this;
    }

    /**
     * Sets the path where the dataset will be written to.
     * @param string The path.
     * @return This, as per the builder pattern.return
     */
    public Builder setDatasetDir(String string) {
      datasetDir = Paths.get(string);
      return this;
    }

    /**
     * Constructs a new {@link DatasetGenerator}.
     * @return A new {@link DatasetGenerator} instance.
     */
    public DatasetGenerator build() {
      return new DatasetGenerator(this);
    }

    static Range<Double> createDynRange(double dynamismLevel) {
      return Range.closedOpen(
        roundDyn(dynamismLevel - DYN_BANDWIDTH),
        roundDyn(dynamismLevel + DYN_BANDWIDTH));
    }

    static double roundDyn(double d) {
      final double pow = Math.pow(10, DYN_PRECISION);
      return Math.round(d * pow) / pow;
    }
  }

  static class IdSeedGenerator {
    final RandomGenerator rng;
    long id;

    Object mutex;
    Set<Long> used;

    IdSeedGenerator(long seed) {
      mutex = new Object();
      rng = new MersenneTwister(seed);
      id = 0L;
      used = new HashSet<>();
    }

    IdSeed next() {
      synchronized (mutex) {
        return IdSeed.create(id++, nextUnique());
      }
    }

    long nextUnique() {
      while (true) {
        final long next = rng.nextLong();
        if (!used.contains(next)) {
          used.add(next);
          return next;
        }
      }
    }
  }

  @AutoValue
  abstract static class IdSeed {
    abstract long getId();

    abstract long getSeed();

    static IdSeed create(long i, long s) {
      return new AutoValue_DatasetGenerator_IdSeed(i, s);
    }
  }

  enum ScenarioComparator implements Comparator<Scenario> {
    INSTANCE {
      @Override
      public int compare(@Nullable Scenario o1, @Nullable Scenario o2) {
        return verifyNotNull(o1).getProblemInstanceId()
          .compareTo(verifyNotNull(o2).getProblemInstanceId());
      }
    }
  }

  enum TimeSeriesType {
    POISSON_SINE, POISSON_HOMOGENOUS, NORMAL, UNIFORM;
  }

  static class CustomTimeWindowGenerator implements TimeWindowGenerator {
    private static final long MINIMAL_PICKUP_TW_LENGTH = 10 * 60 * 1000L;
    private static final long MINIMAL_DELIVERY_TW_LENGTH = 10 * 60 * 1000L;

    private final long urgency;
    private final StochasticSupplier<Double> pickupTWopening;
    private final StochasticSupplier<Double> deliveryTWlength;
    private final StochasticSupplier<Double> deliveryTWopening;
    private final RandomGenerator rng;

    CustomTimeWindowGenerator(long urg) {
      urgency = urg;
      pickupTWopening = StochasticSuppliers.uniformDouble(0d, 1d);
      deliveryTWlength = StochasticSuppliers.uniformDouble(0d, 1d);
      deliveryTWopening = StochasticSuppliers.uniformDouble(0d, 1d);
      rng = new MersenneTwister();
    }

    @Override
    public void generate(long seed, Parcel.Builder parcelBuilder,
        TravelTimes travelTimes, long endTime) {
      rng.setSeed(seed);
      final long orderAnnounceTime = parcelBuilder.getOrderAnnounceTime();
      final Point pickup = parcelBuilder.getPickupLocation();
      final Point delivery = parcelBuilder.getDeliveryLocation();

      final long pickupToDeliveryTT = travelTimes.getShortestTravelTime(pickup,
        delivery);
      final long deliveryToDepotTT = travelTimes
        .getTravelTimeToNearestDepot(delivery);

      // compute range of possible openings
      long pickupOpening;
      if (urgency > MINIMAL_PICKUP_TW_LENGTH) {

        // possible values range from 0 .. n
        // where n = urgency - MINIMAL_PICKUP_TW_LENGTH
        pickupOpening = orderAnnounceTime + DoubleMath.roundToLong(
          pickupTWopening.get(rng.nextLong())
            * (urgency - MINIMAL_PICKUP_TW_LENGTH),
          RoundingMode.HALF_UP);
      } else {
        pickupOpening = orderAnnounceTime;
      }
      final TimeWindow pickupTW = TimeWindow.create(pickupOpening,
        orderAnnounceTime + urgency);
      parcelBuilder.pickupTimeWindow(pickupTW);

      // find boundaries
      final long minDeliveryOpening = pickupTW.begin()
        + parcelBuilder.getPickupDuration() + pickupToDeliveryTT;

      final long maxDeliveryClosing = endTime - deliveryToDepotTT
        - parcelBuilder.getDeliveryDuration();
      long maxDeliveryOpening = maxDeliveryClosing - MINIMAL_DELIVERY_TW_LENGTH;
      if (maxDeliveryOpening < minDeliveryOpening) {
        maxDeliveryOpening = minDeliveryOpening;
      }

      final double openingRange = maxDeliveryOpening - minDeliveryOpening;
      final long deliveryOpening = minDeliveryOpening
        + DoubleMath.roundToLong(deliveryTWopening.get(rng.nextLong())
          * openingRange,
          RoundingMode.HALF_DOWN);

      final long minDeliveryClosing = Math.min(Math.max(pickupTW.end()
        + parcelBuilder.getPickupDuration() + pickupToDeliveryTT,
        deliveryOpening + MINIMAL_DELIVERY_TW_LENGTH), maxDeliveryClosing);

      final double closingRange = maxDeliveryClosing - minDeliveryClosing;
      final long deliveryClosing = minDeliveryClosing
        + DoubleMath.roundToLong(deliveryTWlength.get(rng.nextLong())
          * closingRange,
          RoundingMode.HALF_DOWN);

      final long latestDelivery = endTime - deliveryToDepotTT
        - parcelBuilder.getDeliveryDuration();

      final TimeWindow deliveryTW = TimeWindow.create(deliveryOpening,
        deliveryClosing);

      checkArgument(deliveryOpening >= minDeliveryOpening);
      checkArgument(deliveryOpening + deliveryTW.length() <= latestDelivery);
      checkArgument(pickupTW.end() + parcelBuilder.getPickupDuration()
        + pickupToDeliveryTT <= deliveryOpening + deliveryTW.length());

      parcelBuilder.deliveryTimeWindow(deliveryTW);
    }
  }
}
