/**
 * Copyright (C) 2011 AIM III course DIMA TU Berlin
 *
 * This programm is free software; you can redistribute it and/or modify
 * it under the terms of the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package de.tuberlin.dima.aim3.assignment2;

import static org.junit.Assert.fail;

import com.google.common.collect.Lists;

import de.tuberlin.dima.aim3.HadoopAndPactTestCase;
import de.tuberlin.dima.aim3.assignment2.AverageTemperaturePerMonthPact.TemperatureInFormat;
import de.tuberlin.dima.aim3.assignment2.AverageTemperaturePerMonthPact.TemperatureOutFormat;
import eu.stratosphere.pact.common.contract.Contract;
import eu.stratosphere.pact.common.contract.FileDataSinkContract;
import eu.stratosphere.pact.common.contract.FileDataSourceContract;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.io.FileInputFormat;
import eu.stratosphere.pact.common.io.FileOutputFormat;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactNull;
import eu.stratosphere.pact.common.type.base.PactString;
import eu.stratosphere.pact.testing.TestPlan;
import eu.stratosphere.pact.testing.TestPlanTest;
import eu.stratosphere.pact.testing.ioformats.JsonInputFormat;

import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Enumeration;
import java.util.List;
import java.util.regex.Pattern;

public class AverageTemperaturePerMonthPactTest extends HadoopAndPactTestCase {

  private static final Pattern SEPARATOR = Pattern.compile("\t");

  @Test
  public void computeTemperatures() throws IOException {

	final FileDataSourceContract<PactNull, PactString> dataSourceContract =
		createInput(TemperatureInFormat.class, "assignment1/temperatures.tsv");
	  
    final MapContract<PactNull, PactString, AverageTemperaturePerMonthPact.YearMonthKey, PactInteger> mapContract =
        new MapContract<PactNull, PactString, AverageTemperaturePerMonthPact.YearMonthKey, PactInteger>(
        AverageTemperaturePerMonthPact.TemperaturePerYearAndMonthMapper.class);

    final ReduceContract<AverageTemperaturePerMonthPact.YearMonthKey, PactInteger,
        AverageTemperaturePerMonthPact.YearMonthKey, PactDouble> reduceContract =
        new ReduceContract<AverageTemperaturePerMonthPact.YearMonthKey, PactInteger,
        AverageTemperaturePerMonthPact.YearMonthKey, PactDouble>(
        AverageTemperaturePerMonthPact.TemperatePerYearAndMonthReducer.class);

    reduceContract.setInput(mapContract);
    mapContract.setInput(dataSourceContract);
    
    FileDataSinkContract<AverageTemperaturePerMonthPact.YearMonthKey, PactDouble> dataSinkContract =
        	createOutput(reduceContract, TemperatureOutFormat.class);

    TestPlan testPlan = new TestPlan(dataSinkContract);
    testPlan.getExpectedOutput(dataSinkContract).fromFile(TemperatureInFormat.class, getResourcePath("assignment1/averageTemperatures.tsv"));

//    for (String line : readLines("/assignment1/temperatures.tsv")) {
//      testPlan.getInput().add(PactNull.getInstance(), new PactString(line));
//    }
//
//    testPlan.setAllowedPactDoubleDelta(0.0001);
//    for (KeyValuePair<AverageTemperaturePerMonthPact.YearMonthKey, PactDouble> expectedResult : expectedResults()) {
//      testPlan.getExpectedOutput().add(expectedResult.getKey(), expectedResult.getValue());
//    }

    testPlan.run();
  }

//  Iterable<KeyValuePair<AverageTemperaturePerMonthPact.YearMonthKey, PactDouble>> expectedResults() throws IOException {
//    List<KeyValuePair<AverageTemperaturePerMonthPact.YearMonthKey, PactDouble>> results = Lists.newArrayList();
//    for (String line : readLines("/assignment1/averageTemperatures.tsv")) {
//      String[] tokens = SEPARATOR.split(line);
//      results.add(new KeyValuePair<AverageTemperaturePerMonthPact.YearMonthKey, PactDouble>(
//          new AverageTemperaturePerMonthPact.YearMonthKey(new PactInteger(Integer.parseInt(tokens[0])), new PactInteger(Integer.parseInt(tokens[1]))),
//          new PactDouble(Double.parseDouble(tokens[2]))));
//    }
//    return results;
//  }

	private <K extends Key, V extends Value> FileDataSourceContract<K, V> createInput(
			Class<? extends FileInputFormat<K, V>> inputFormat, String resource) {
		final FileDataSourceContract<K, V> read = new FileDataSourceContract<K, V>(inputFormat, getResourcePath(resource),
			"Input");
		return read;
	}

	private String getResourcePath(String resource) {
		try {
			Enumeration<URL> resources = AverageTemperaturePerMonthPactTest.class.getClassLoader().getResources(resource);
			if (resources.hasMoreElements())
				return resources.nextElement().toString();
		} catch (IOException e) {
			throw new IllegalStateException(e);
		}
		throw new IllegalArgumentException("no resources found");
	}
	
	private <K extends Key, V extends Value> FileDataSinkContract<K, V> createOutput(final Contract input,
			final Class<? extends FileOutputFormat<K, V>> outputFormatClass) {
		try {
			final FileDataSinkContract<K, V> out = new FileDataSinkContract<K, V>(outputFormatClass, File.createTempFile(
				"output", null).toURI().toString(), "Output");
			out.setInput(input);
			return out;
		} catch (IOException e) {
			fail("cannot create temporary output file" + e);
			return null;
		}
	}
}
