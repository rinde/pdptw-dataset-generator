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
package com.github.rinde.dataset;

import static com.github.rinde.rinsim.util.StochasticSuppliers.constant;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verifyNotNull;

import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
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

import com.github.rinde.rinsim.core.model.pdp.DefaultPDPModel;
import com.github.rinde.rinsim.core.model.pdp.Parcel;
import com.github.rinde.rinsim.core.model.pdp.TimeWindowPolicy.TimeWindowPolicies;
import com.github.rinde.rinsim.core.model.road.RoadModelBuilders;
import com.github.rinde.rinsim.core.model.time.TimeModel;
import com.github.rinde.rinsim.geom.Point;
import com.github.rinde.rinsim.pdptw.common.PDPRoadModel;
import com.github.rinde.rinsim.pdptw.common.StatsStopConditions;
import com.github.rinde.rinsim.scenario.Scenario;
import com.github.rinde.rinsim.scenario.Scenario.ProblemClass;
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
import com.github.rinde.rinsim.util.StochasticSupplier;
import com.github.rinde.rinsim.util.StochasticSuppliers;
import com.github.rinde.rinsim.util.TimeWindow;
import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableRangeMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.LinkedHashMultimap;
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
 * @author Rinde van Lon
 *
 */
public class DatasetGenerator {

  private static final long TICK_SIZE = 1000L;
  private static final double VEHICLE_SPEED_KMH = 50d;

  // n x n (km)
  private static final double AREA_WIDTH = 10;

  private static final long SCENARIO_HOURS = 12L;
  private static final long SCENARIO_LENGTH = SCENARIO_HOURS * 60 * 60 * 1000L;
  private static final int NUM_ORDERS = 360;

  private static final long HALF_DIAG_TT = 509117L;
  private static final long ONE_AND_HALF_DIAG_TT = 1527351L;
  private static final long TWO_DIAG_TT = 2036468L;

  private static final long PICKUP_DURATION = 5 * 60 * 1000L;
  private static final long DELIVERY_DURATION = 5 * 60 * 1000L;

  private static final long INTENSITY_PERIOD = 60 * 60 * 1000L;

  // These parameters influence the dynamism selection settings
  private static final double DYN_STEP_SIZE = 0.05;
  private static final double DYN_BANDWIDTH = 0.01;
  // number of digits
  private static final double DYN_PRECISION = 2;

  final Builder builder;

  DatasetGenerator(Builder b) {
    builder = b;
  }

  Dataset<GeneratedScenario> generate() {

    final ListeningExecutorService service = MoreExecutors
      .listeningDecorator(Executors.newFixedThreadPool(builder.numThreads));
    final Dataset<GeneratedScenario> dataset = Dataset.naturalOrder();

    final List<ScenarioCreator> jobs = new ArrayList<>();

    final RandomGenerator rng = new MersenneTwister(builder.randomSeed);
    final Map<GeneratorSettings, IdSeedGenerator> rngMap = new LinkedHashMap<>();

    for (final Long urgency : builder.urgencyLevels) {
      for (final Double scale : builder.scaleLevels) {
        for (final Entry<TimeSeriesType, Collection<Range<Double>>> dynLevel : builder.dynamismLevels
          .asMap().entrySet()) {

          System.out.println(dynLevel);

          final int reps = builder.numInstances * dynLevel.getValue().size();

          final long urg = urgency * 60 * 1000L;
          // The office hours is the period in which new orders are accepted,
          // it is defined as [0,officeHoursLength).
          final long officeHoursLength;
          if (urg < HALF_DIAG_TT) {
            officeHoursLength = SCENARIO_LENGTH - TWO_DIAG_TT
              - PICKUP_DURATION
              - DELIVERY_DURATION;
          } else {
            officeHoursLength = SCENARIO_LENGTH - urg
              - ONE_AND_HALF_DIAG_TT
              - PICKUP_DURATION - DELIVERY_DURATION;
          }

          final int numOrders = DoubleMath.roundToInt(scale * NUM_ORDERS,
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

          final TimeSeriesGenerator tsg = createTimeSeriesGenerator(
            dynLevel.getKey(), officeHoursLength, numOrders, props);

          final GeneratorSettings set = GeneratorSettings
            .builder()
            .setDayLength(SCENARIO_LENGTH)
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
              ImmutableMap.<String, String> builder());
            final ScenarioGenerator gen = createGenerator(SCENARIO_LENGTH,
              urg, scale, tsg2, lg);

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
        Thread.sleep(100);
      } catch (final InterruptedException e) {
        e.printStackTrace();
      }
    }

    System.out.println("STOPPING");
    service.shutdown();
    try {
      service.awaitTermination(1L, TimeUnit.HOURS);
    } catch (final InterruptedException e) {
      e.printStackTrace();
    }

    return dataset;
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

  static Dataset<Scenario> convert(Dataset<GeneratedScenario> input) {
    final Dataset<Scenario> data = Dataset
      .orderedBy(ScenarioComparator.INSTANCE);
    for (final GeneratedScenario gs : input) {
      final GeneratorSettings settings = gs.getSettings();

      final double d = 0d;

      final int cur = data.get(d, settings.getUrgency(), settings.getScale())
        .size();
      final ProblemClass pc = VanLon15ProblemClass.create(d,
        settings.getUrgency(), settings.getScale());
      final Scenario finalScenario = Scenario.builder(pc)
        .copyProperties(gs.getScenario())
        .problemClass(pc)
        .instanceId(Integer.toString(cur))
        .build();

      data.put(d, settings.getUrgency(), settings.getScale(), finalScenario);
    }
    return data;
  }

  static void submitJob(final AtomicLong currentJobs,
    final ListeningExecutorService service,
    final ScenarioCreator job,
    final int numInstances,
    final Dataset<GeneratedScenario> dataset,
    final Map<GeneratorSettings, IdSeedGenerator> rngMap,
    final AtomicLong datasetSize) {

    // System.out.println(datasetSize);
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
        System.out.println("ok");
        if (dataset.get(res.getDynamismBin(),
          res.getSettings().getUrgency(),
          res.getSettings().getScale()).size() < numInstances) {

          datasetSize.getAndIncrement();
          dataset.put(res.getDynamismBin(),
            res.getSettings().getUrgency(),
            res.getSettings().getScale(),
            res);
        } else {
          System.out.println("we already have enough of: "
            + res.getDynamismBin() + " "
            + res.getSettings().getTimeSeriesType());

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
              rngMap.get(job.getSettings()).next()
              , job.getSettings(),
              job.getGenerator());

            if (!service.isShutdown()) {
              submitJob(currentJobs, service, newJob, numInstances,
                dataset, rngMap, datasetSize);
            }
          }
        }

        // result.

        // dataset.put(job.getDynamism(), job.getUrgency(), job.getScale(),
        // verifyNotNull(result));
      }

      @Override
      public void onFailure(Throwable t) {
        throw new IllegalStateException(t);
        // System.err.println("fail: " + t);
        // t.printStackTrace();
        // add new job (or perhaps 2, to increase odds?)

      }
    }, MoreExecutors.directExecutor());
  }

  static TimeSeriesGenerator createTimeSeriesGenerator(TimeSeriesType type,
    long officeHoursLength, int numOrders,
    ImmutableMap.Builder<String, String> props) {

    final double numPeriods = officeHoursLength / (double) INTENSITY_PERIOD;
    if (type == TimeSeriesType.POISSON_SINE) {
      props.put("time_series", "sine Poisson ");
      props.put("time_series.period", Long.toString(INTENSITY_PERIOD));
      props.put("time_series.num_periods", Double.toString(numPeriods));

      final TimeSeriesGenerator sineTsg = TimeSeries.nonHomogenousPoisson(
        officeHoursLength,
        IntensityFunctions
          .sineIntensity()
          .area(numOrders / numPeriods)
          .period(INTENSITY_PERIOD)
          .height(StochasticSuppliers.uniformDouble(-.99, 3d))
          .phaseShift(
            StochasticSuppliers.uniformDouble(0, INTENSITY_PERIOD))
          .buildStochasticSupplier());
      return sineTsg;
    } else if (type == TimeSeriesType.POISSON_HOMOGENOUS) {
      props.put("time_series", "homogenous Poisson");
      props.put("time_series.intensity",
        Double.toString((double) NUM_ORDERS / (double) officeHoursLength));
      return TimeSeries.homogenousPoisson(officeHoursLength, numOrders);
    } else if (type == TimeSeriesType.NORMAL) {
      props.put("time_series", "normal");
      return TimeSeries.normal(officeHoursLength, numOrders, 2.4 * 60 * 1000);
    } else if (type == TimeSeriesType.UNIFORM) {
      props.put("time_series", "uniform");
      final StochasticSupplier<Double> maxDeviation = StochasticSuppliers
        .normal()
        .mean(1 * 60 * 1000)
        .std(1 * 60 * 1000)
        .lowerBound(0)
        .upperBound(15d * 60 * 1000)
        .buildDouble();
      return TimeSeries.uniform(
        officeHoursLength, numOrders, maxDeviation);
    }
    throw new IllegalStateException();
  }

  enum ScenarioComparator implements Comparator<Scenario> {
    INSTANCE {
      @Override
      public int compare(@Nullable Scenario o1, @Nullable Scenario o2) {
        return verifyNotNull(o1).getProblemInstanceId().compareTo(
          verifyNotNull(o2).getProblemInstanceId());
      }

    }
  }

  // Generator.builder().withScale(10).withDynamism(.4).withUrgency(70).instances(10).generate()

  // withScale( 0, 1, 2, 3)
  // withDynamism(.4,.5)

  // multi-threaded
  enum TimeSeriesType {
    POISSON_SINE, POISSON_HOMOGENOUS, NORMAL, UNIFORM;
  }

  public static Builder builder() {
    return new Builder();
  }

  static class Builder {
    static final ImmutableRangeMap<Double, TimeSeriesType> DYNAMISM_MAP =
      ImmutableRangeMap.<Double, TimeSeriesType> builder()
        .put(Range.closedOpen(0.000, 0.475), TimeSeriesType.POISSON_SINE)
        .put(Range.closedOpen(0.475, 0.575), TimeSeriesType.POISSON_HOMOGENOUS)
        .put(Range.closedOpen(0.575, 0.675), TimeSeriesType.NORMAL)
        .put(Range.closedOpen(0.675, 1.000), TimeSeriesType.UNIFORM)
        .build();

    long randomSeed;
    ImmutableSet<Double> scaleLevels;
    ImmutableSetMultimap<TimeSeriesType, Range<Double>> dynamismLevels;
    ImmutableRangeMap<Double, Double> dynamismRangeMap;
    ImmutableSet<Long> urgencyLevels;
    int numInstances;
    int numThreads;

    Builder() {
      randomSeed = 0L;
      scaleLevels = ImmutableSet.of(1d);
      dynamismLevels = ImmutableSetMultimap.of(
        TimeSeriesType.POISSON_HOMOGENOUS, createDynRange(.5));
      dynamismRangeMap = ImmutableRangeMap.of(createDynRange(.5), .5);
      urgencyLevels = ImmutableSet.of(20L);
      numInstances = 1;
      numThreads = Runtime.getRuntime().availableProcessors();
    }

    public Builder setRandomSeed(long seed) {
      randomSeed = seed;
      return this;
    }

    public Builder setScaleLevels(Iterable<Double> levels) {
      checkArgument(Iterables.size(levels) > 0);
      for (final Double d : levels) {
        checkArgument(d > 0d);
      }
      scaleLevels = ImmutableSet.copyOf(levels);
      return this;
    }

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
        LinkedHashMultimap.<TimeSeriesType, Range<Double>> create();

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

    public Builder setNumInstances(int num) {
      numInstances = num;
      return this;
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

    /**
     * Urgency is expressed in minutes.
     * @param levels
     * @return
     */
    public Builder setUrgencyLevels(Iterable<Long> levels) {
      checkArgument(Iterables.size(levels) > 0);
      for (final Long l : levels) {
        checkArgument(l > 0);
      }
      urgencyLevels = ImmutableSet.copyOf(levels);
      return this;
    }

    public DatasetGenerator build() {
      return new DatasetGenerator(this);
    }

    public Builder setNumThreads(int i) {
      numThreads = i;
      return this;
    }
  }

  static ScenarioGenerator createGenerator(long scenarioLength,
    long urgency, double scale, TimeSeriesGenerator tsg, LocationGenerator lg) {
    return ScenarioGenerator.builder()

      // global
      .addModel(TimeModel.builder()
        .withTickLength(TICK_SIZE)
        .withTimeUnit(SI.MILLI(SI.SECOND))
      )
      .scenarioLength(scenarioLength)
      .setStopCondition(StopConditions.and(
        StatsStopConditions.vehiclesDoneAndBackAtDepot(),
        StatsStopConditions.timeOutEvent()
        )
      )
      // parcels
      .parcels(
        Parcels
          .builder()
          .announceTimes(
            TimeSeries.filter(tsg, TimeSeries.numEventsPredicate(
              DoubleMath.roundToInt(NUM_ORDERS * scale,
                RoundingMode.UNNECESSARY)
              ))
          )
          .pickupDurations(constant(PICKUP_DURATION))
          .deliveryDurations(constant(DELIVERY_DURATION))
          .neededCapacities(constant(0))
          .locations(lg)
          .timeWindows(new CustomTimeWindowGenerator(urgency)
          // TimeWindows.builder()
          // .pickupUrgency(constant(urgency))
          // // .pickupTimeWindowLength(StochasticSuppliers.uniformLong(5
          // // * 60 * 1000L,))
          // .deliveryOpening(constant(0L))
          // .minDeliveryLength(constant(10 * 60 * 1000L))
          // .deliveryLengthFactor(constant(3d))
          // .build()
          )
          .build())

      // vehicles
      .vehicles(
        Vehicles
          .builder()
          .capacities(constant(1))
          .centeredStartPositions()
          .creationTimes(constant(-1L))
          .numberOfVehicles(
            constant(DoubleMath
              .roundToInt(10 * scale, RoundingMode.UNNECESSARY)))
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
            .withDistanceUnit(SI.KILOMETER)
          )
          .withAllowVehicleDiversion(true)
      )
      .addModel(
        DefaultPDPModel.builder()
          .withTimeWindowPolicy(TimeWindowPolicies.TARDY_ALLOWED)
      )
      .build();
  }

  static class CustomTimeWindowGenerator implements TimeWindowGenerator {
    private static final long MINIMAL_PICKUP_TW_LENGTH = 10 * 60 * 1000L;
    private static final long MINIMAL_DELIVERY_TW_LENGTH = 10 * 60 * 1000L;

    private final long urgency;
    private final StochasticSupplier<Double> pickupTWopening;
    private final StochasticSupplier<Double> deliveryTWlength;
    private final StochasticSupplier<Double> deliveryTWopening;
    private final RandomGenerator rng;

    public CustomTimeWindowGenerator(long urg) {
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
            * (urgency - MINIMAL_PICKUP_TW_LENGTH), RoundingMode.HALF_UP);
      } else {
        pickupOpening = orderAnnounceTime;
      }
      final TimeWindow pickupTW = new TimeWindow(pickupOpening,
        orderAnnounceTime + urgency);
      parcelBuilder.pickupTimeWindow(pickupTW);

      // find boundaries
      final long minDeliveryOpening = pickupTW.begin
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
          * openingRange, RoundingMode.HALF_DOWN);

      final long minDeliveryClosing = Math.min(Math.max(pickupTW.end
        + parcelBuilder.getPickupDuration() + pickupToDeliveryTT,
        deliveryOpening + MINIMAL_DELIVERY_TW_LENGTH), maxDeliveryClosing);

      final double closingRange = maxDeliveryClosing - minDeliveryClosing;
      final long deliveryClosing = minDeliveryClosing
        + DoubleMath.roundToLong(deliveryTWlength.get(rng.nextLong())
          * closingRange, RoundingMode.HALF_DOWN);

      final long latestDelivery = endTime - deliveryToDepotTT
        - parcelBuilder.getDeliveryDuration();

      // final long minDeliveryTWlength = MINIMAL_DELIVERY_TW_LENGTH;
      // // Math
      // // .max(MINIMAL_DELIVERY_TW_LENGTH,
      // // pickupTW.end + parcelBuilder.getPickupDuration()
      // // + pickupToDeliveryTT);
      // final long maxDeliveryTWlength = latestDelivery - minDeliveryOpening;
      //
      // double factor = maxDeliveryTWlength - minDeliveryTWlength;
      // if (factor < 0d) {
      // factor = 0;
      // }
      // long deliveryTimeWindowLength = minDeliveryTWlength
      // + DoubleMath.roundToLong(deliveryTWlength.get(rng.nextLong())
      // * factor, RoundingMode.HALF_UP);
      //
      // // delivery TW may not close before this time:
      // final long minDeliveryClosing = pickupTW.end
      // + parcelBuilder.getPickupDuration() + pickupToDeliveryTT;
      //
      // if (minDeliveryOpening < minDeliveryClosing - deliveryTimeWindowLength)
      // {
      // minDeliveryOpening = minDeliveryClosing - deliveryTimeWindowLength;
      // }
      //
      // final long deliveryOpening;
      // if (deliveryTimeWindowLength >= maxDeliveryTWlength) {
      // deliveryOpening = minDeliveryOpening;
      // deliveryTimeWindowLength = maxDeliveryTWlength;
      // } else {
      // deliveryOpening = minDeliveryOpening
      // + DoubleMath.roundToLong(deliveryTWopening.get(rng.nextLong())
      // * (maxDeliveryTWlength - deliveryTimeWindowLength),
      // RoundingMode.HALF_UP);
      // }
      //
      // if (deliveryOpening + deliveryTimeWindowLength > latestDelivery) {
      // deliveryTimeWindowLength = latestDelivery - deliveryOpening;
      // }

      final TimeWindow deliveryTW = new TimeWindow(deliveryOpening,
        deliveryClosing);

      checkArgument(deliveryOpening >= minDeliveryOpening);
      checkArgument(deliveryOpening + deliveryTW.length() <= latestDelivery);
      checkArgument(pickupTW.end + parcelBuilder.getPickupDuration()
        + pickupToDeliveryTT <= deliveryOpening + deliveryTW.length());

      parcelBuilder.deliveryTimeWindow(deliveryTW);
    }
  }
}
