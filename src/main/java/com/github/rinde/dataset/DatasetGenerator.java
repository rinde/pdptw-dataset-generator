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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verifyNotNull;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.Nullable;

import org.apache.commons.math3.random.MersenneTwister;
import org.apache.commons.math3.random.RandomGenerator;

import com.github.rinde.rinsim.scenario.Scenario;
import com.github.rinde.rinsim.scenario.Scenario.ProblemClass;
import com.github.rinde.rinsim.scenario.generator.ScenarioGenerator;
import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableRangeMap;
import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Ordering;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.RowSortedTable;
import com.google.common.collect.TreeBasedTable;
import com.google.common.collect.TreeRangeSet;
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

  private static final int TARGET_NUM_INSTANCES = 50;

  // These parameters influence the dynamism selection settings
  private static final double DYN_STEP_SIZE = 0.05;
  private static final double DYN_BANDWIDTH = 0.01;
  // number of digits
  private static final double DYN_PRECISION = 2;

  final Builder builder;

  DatasetGenerator(Builder b) {
    builder = b;
  }

  // static class Run implements Callable<Scenario> {
  //
  // long instanceNumber;
  //
  // @Override
  // public Scenario call() throws Exception {
  // // TODO Auto-generated method stub
  // return null;
  // }
  //
  // }

  static class Dataset<T> implements Iterable<T> {
    Comparator<T> comparator;
    SortedMap<Double, RowSortedTable<Long, Double, SortedSet<T>>> data;

    private Dataset(Comparator<T> comp) {
      comparator = comp;

      data = new TreeMap<Double, RowSortedTable<Long, Double, SortedSet<T>>>();
    }

    static <T extends Comparable<T>> Dataset<T> naturalOrder() {
      return new Dataset<>(Ordering.<T> natural());
    }

    static <T> Dataset<T> orderedBy(Comparator<T> comparator) {
      return new Dataset<>(comparator);
    }

    public void put(double dyn, long urg, double scl, T value) {
      if (!data.containsKey(dyn)) {
        data.put(dyn, TreeBasedTable.<Long, Double, SortedSet<T>> create());
      }
      if (!data.get(dyn).contains(urg, scl)) {
        data.get(dyn).put(urg, scl, new TreeSet<>(comparator));
      }
      checkArgument(!data.get(dyn).get(urg, scl).contains(value));
      data.get(dyn).get(urg, scl).add(value);
    }

    public boolean containsEntry(double dyn, long urg, double scl, T value) {
      return data.containsKey(dyn)
        && data.get(dyn).contains(urg, scl)
        && data.get(dyn).get(urg, scl).contains(value);
    }

    public SortedSet<T> get(double dyn, long urg, double scl) {
      if (!data.containsKey(dyn) || !data.get(dyn).contains(urg, scl)) {
        return ImmutableSortedSet.of();
      }
      return Collections.unmodifiableSortedSet(data.get(dyn).get(urg, scl));
    }

    @Override
    public Iterator<T> iterator() {
      final List<Iterator<T>> its = new ArrayList<>();
      for (final Entry<Double, RowSortedTable<Long, Double, SortedSet<T>>> entry : data
        .entrySet()) {
        for (final SortedSet<T> set : entry.getValue().values()) {
          its.add(set.iterator());
        }
      }
      return Iterators.concat(its.iterator());
    }
  }

  @AutoValue
  abstract static class VanLon15ProblemClass implements ProblemClass {
    public abstract double getDynamism();

    public abstract long getUrgency();

    public abstract double getScale();

    @Override
    public String getId() {
      return String.format("%1.2f-%d-%1.2f",
        getDynamism(),
        getUrgency(),
        getScale());
    }

    static VanLon15ProblemClass create(double dyn, long urg, double scl) {
      return new AutoValue_DatasetGenerator_VanLon15ProblemClass(dyn, urg, scl);
    }
  }

  @AutoValue
  abstract static class VanLonScenario extends Scenario {

    public abstract double getDynamism();

    public abstract long getUrgency();

    public abstract double getScale();

    @Override
    public abstract VanLon15ProblemClass getProblemClass();

    // static VanLonScenario create(Scenario s, double dyn, long urg, double
    // scl) {
    // return new AutoValue_DatasetGenerator_VanLonScenario(
    // s.getEvents(),
    // s.getModelBuilders(),
    // s.getTimeWindow(),
    // s.getStopCondition(),
    // (VanLon15ProblemClass) s.getProblemClass(),
    // s.getProblemInstanceId(),
    // dyn,
    // urg,
    // scl);
    // }
  }

  @AutoValue
  abstract static class GeneratedScenario implements
    Comparable<GeneratedScenario> {

    public abstract Scenario getScenario();

    public abstract long getId();

    @Override
    public int compareTo(@Nullable GeneratedScenario scen) {
      return Long.compare(getId(), verifyNotNull(scen).getId());
    }

    static GeneratedScenario create(Scenario s, long id) {
      return new AutoValue_DatasetGenerator_GeneratedScenario(s, id);
    }
  }

  @AutoValue
  abstract static class ScenGen implements Callable<GeneratedScenario> {

    @Override
    public GeneratedScenario call() throws Exception {
      final RandomGenerator rng = new MersenneTwister(getSettings().getSeed());
      return GeneratedScenario.create(getGenerator().generate(rng, ""), 0L);
    }

    public abstract long getId();

    public abstract GeneratorSettings getSettings();

    public abstract ScenarioGenerator getGenerator();

    static ScenGen create(long id, double dyn, long urg, double scl) {
      return new AutoValue_DatasetGenerator_ScenGen(id, dyn, urg, scl);
    }
  }

  @AutoValue
  abstract static class GeneratorSettings {

    public abstract long getSeed();

    public abstract double getScale();

    public abstract TimeSeriesType getTimeSeriesType();

    public abstract ImmutableRangeSet<Double> getAllowedRanges();

    public abstract ImmutableRangeMap<Double, Double> getRangeCenters();

    public abstract long getUrgency();

    public abstract long getDayLength();

    public abstract long getOfficeHours();

    public abstract ImmutableMap<String, String> getProperties();

    // static GeneratorSettings create() {
    //
    // }
  }

  // enum ScenGenComparator implements Comparator<ScenGen> {
  // INSTANCE {
  // @Override
  // public int compare(@Nullable ScenGen o1, @Nullable ScenGen o2) {
  // return Long.compare(
  // verifyNotNull(o1).getInstanceId(),
  // verifyNotNull(o2).getInstanceId());
  // }
  // }
  // }

  ImmutableSet<Scenario> generate() {

    final ListeningExecutorService service = MoreExecutors
      .listeningDecorator(Executors.newFixedThreadPool(Runtime.getRuntime()
        .availableProcessors()));
    final Dataset<GeneratedScenario> dataset = Dataset.naturalOrder();

    final AtomicLong id = new AtomicLong(0L);
    for (final Range<Double> dynamism : builder.dynamismLevels) {
      for (final Double scale : builder.scaleLevels) {
        for (final Long urgency : builder.urgencyLevels) {

          // - dynamism generator method (SINE,HOMOGENOUS,..)

          // need to supply:
          // - random seed
          // - ScenarioGenerator
          // - Predicate that checks following properties
          // -- dynamism range map that is allowed (should be subset of
          // generator method). Range<Double> -> Double
          // -- urgency
          // -- scale

          // ScenGen.create(id.getAndIncrement(), dyn, urg, scl)

        }
      }
    }
    final ScenGen job = ScenGen.create(1L, .5, 10, 1d);
    final ListenableFuture<GeneratedScenario> future = service.submit(job);
    Futures.addCallback(future, new FutureCallback<GeneratedScenario>() {
      @Override
      public void onSuccess(@Nullable GeneratedScenario result) {
        // dataset.put(job.getDynamism(), job.getUrgency(), job.getScale(),
        // verifyNotNull(result));
      }

      @Override
      public void onFailure(Throwable t) {
        // add new job

      }
    });

    // final RangeMap<Double, > dynamismMap = TreeRangeMap.create();

    // scenGenSettings (runnable?)
    // int instanceNr
    // double scale
    // long urgency

    // for (final Double dynamism : builder.dynamismLevels) {
    // for (final Double scale : builder.scaleLevels) {
    // for (final Long urgency : builder.urgencyLevels) {
    //
    // // create a range map based on dynamism levels
    // // associate all scenarios with a certain dynamism range
    // // is useful for counting them
    //
    // // while (numInstances < builder.numInstances) {
    // //
    // // }
    //
    // }
    // }
    // }
    return ImmutableSet.of();

  }

  // Generator.builder().withScale(10).withDynamism(.4).withUrgency(70).instances(10).generate()

  // withScale( 0, 1, 2, 3)
  // withDynamism(.4,.5)

  // multi-threaded
  enum TimeSeriesType {
    SINE, HOMOGENOUS, NORMAL, UNIFORM;
  }

  public static Builder builder() {
    return new Builder();
  }

  static class Builder {
    static final ImmutableRangeMap<Double, TimeSeriesType> DYNAMISM_MAP =
      ImmutableRangeMap.<Double, TimeSeriesType> builder()
        .put(Range.closedOpen(0.000, 0.475), TimeSeriesType.SINE)
        .put(Range.closedOpen(0.475, 0.575), TimeSeriesType.HOMOGENOUS)
        .put(Range.closedOpen(0.575, 0.675), TimeSeriesType.NORMAL)
        .put(Range.closedOpen(0.675, 1.000), TimeSeriesType.UNIFORM)
        .build();

    long randomSeed;
    ImmutableSet<Double> scaleLevels;
    ImmutableSet<Range<Double>> dynamismLevels;
    ImmutableSet<Long> urgencyLevels;
    int numInstances;

    Builder() {
      randomSeed = 0L;
      scaleLevels = ImmutableSet.of(1d);
      dynamismLevels = ImmutableSet.of(createDynRange(.5));
      urgencyLevels = ImmutableSet.of(20L);
      numInstances = 1;
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
      final ImmutableSet.Builder<Range<Double>> dynamismLevelsB = ImmutableSet
        .builder();
      for (final Double d : levels) {
        checkArgument(d >= 0d && d <= 1d);
        final Range<Double> newRange = createDynRange(d);
        checkArgument(
          rangeSet.subRangeSet(newRange).isEmpty(),
          "Can not add dynamism level %s, it is too close to another level.",
          d);
        rangeSet.add(newRange);
        dynamismLevelsB.add(newRange);
      }

      dynamismLevels = dynamismLevelsB.build();
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
  }
}
