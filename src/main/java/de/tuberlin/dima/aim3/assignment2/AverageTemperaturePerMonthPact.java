package de.tuberlin.dima.aim3.assignment2;

import eu.stratosphere.pact.common.io.TextInputFormat;
import eu.stratosphere.pact.common.io.TextOutputFormat;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.MapStub;
import eu.stratosphere.pact.common.stub.ReduceStub;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactNull;
import eu.stratosphere.pact.common.type.base.PactPair;
import eu.stratosphere.pact.common.type.base.PactString;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

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

  public static class TemperatureInFormat extends TextInputFormat<PactNull, PactString> {

		@Override
		public boolean readLine(KeyValuePair<PactNull, PactString> pair, byte[] line) {
			pair.setKey(new PactNull());
			pair.setValue(new PactString(new String(line)));
			return true;
		}
	}
  
  public static class TemperatureOutFormat extends TextOutputFormat<YearMonthKey, PactDouble> {

		@Override
		public byte[] writeLine(KeyValuePair<YearMonthKey, PactDouble> pair) {
			String key = pair.getKey().toString();
			String value = pair.getValue().toString();
			String line = key + "\t" + value + "\n";
			return line.getBytes();
		}
	}
  
  public static class TemperaturePerYearAndMonthMapper
      extends MapStub<PactNull, PactString, YearMonthKey, PactInteger> {

    @Override
    public void map(PactNull pactNull, PactString line, Collector<YearMonthKey, PactInteger> collector) {

    	double minimumQuality = 0.25;
		String measurement = line.toString();
		
		StringTokenizer tokenizer = new StringTokenizer(measurement);
		while (tokenizer.hasMoreElements()) {
			int year = (Integer) tokenizer.nextElement();
			int month = (Integer) tokenizer.nextElement();
			int temperature = (Integer) tokenizer.nextElement();
			double quality = (Double) tokenizer.nextElement();
			if (quality >= minimumQuality) {
				collector.collect(new YearMonthKey(new PactInteger(year), new PactInteger(month)), new PactInteger(temperature));
			}
		}
    }
  }

  public static class TemperatePerYearAndMonthReducer
      extends ReduceStub<YearMonthKey, PactInteger, YearMonthKey, PactDouble> {

    @Override
    public void reduce(YearMonthKey yearMonthKey, Iterator<PactInteger> temperatures,
        Collector<YearMonthKey, PactDouble> collector) {
    	int count = 0;
		int sum = 0;
		while (temperatures.hasNext()) {
			count++;
			PactInteger element = (PactInteger) temperatures.next();
			sum += element.getValue();
		}
		double average = (double) sum / (double) count;
		collector.collect(yearMonthKey, new PactDouble(average));
    }
  }
  
	public static class YearMonthKey extends PactPair<PactInteger, PactInteger> {

		public YearMonthKey() {
			super();
		}

		public YearMonthKey(PactInteger i1, PactInteger i2) {
			super(i1, i2);
		}

		public String toString() {
			return getFirst().toString() + "\t" + getSecond().toString();
		}
	}
}