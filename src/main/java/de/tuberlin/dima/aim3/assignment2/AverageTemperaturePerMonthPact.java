package de.tuberlin.dima.aim3.assignment2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.MapStub;
import eu.stratosphere.pact.common.stub.ReduceStub;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactNull;
import eu.stratosphere.pact.common.type.base.PactString;

public class AverageTemperaturePerMonthPact implements PlanAssembler, PlanAssemblerDescription {

  public static final double MINIMUM_QUALITY = 0.25;

  @Override
  public String getDescription() {
    return null;
  }

  @Override
  public Plan getPlan(String... args) throws IllegalArgumentException {
    //IMPLEMENT ME
    return null;
  }
  
  public static class TemperaturePerYearAndMonthMapper
      extends MapStub<PactNull, PactString, YearMonthKey, PactInteger> {

    @Override
    public void map(PactNull pactNull, PactString line, Collector<YearMonthKey, PactInteger> collector) {
      //IMPLEMENT ME
    }
  }

  public static class TemperatePerYearAndMonthReducer
      extends ReduceStub<YearMonthKey, PactInteger, YearMonthKey, PactDouble> {

    @Override
    public void reduce(YearMonthKey yearMonthKey, Iterator<PactInteger> temperatures,
        Collector<YearMonthKey, PactDouble> collector) {
      // IMPLEMENT
    }
  }

  public static class YearMonthKey implements Key {

    public YearMonthKey() {}

    public YearMonthKey(short year, short month) {
      //IMPLEMENT
    }

    @Override
    public int compareTo(Key other) {
      // IMPLEMENT
      return 0;
    }

    @Override
    public void write(DataOutput out) throws IOException {
      //IMPLEMENT
    }

    @Override
    public void read(DataInput in) throws IOException {
      //IMPLEMENT
    }

    //IMPLEMENT equals() and hashCode()
  }
}