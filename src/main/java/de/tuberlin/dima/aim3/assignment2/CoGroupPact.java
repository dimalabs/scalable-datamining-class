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

import java.util.Iterator;

import eu.stratosphere.pact.common.io.TextInputFormat;
import eu.stratosphere.pact.common.io.TextOutputFormat;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription;
import eu.stratosphere.pact.common.stub.CoGroupStub;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactString;

public class CoGroupPact implements PlanAssembler, PlanAssemblerDescription {

  public static class CoGroupPactInFormat extends TextInputFormat<PactString, PactString> {
    @Override
    public boolean readLine(KeyValuePair<PactString, PactString> pair, byte[] line) {
      //IMPLEMENT ME
      return true;
    }
  }

  public static class CoGroupOutFormat extends TextOutputFormat<PactString, PactInteger> {
    @Override
    public byte[] writeLine(KeyValuePair<PactString, PactInteger> pair) {
      //IMPLEMENT ME
      return null;
    }
  }

  public static class TestCoGrouper extends CoGroupStub<PactString, PactString, PactString, PactString, PactInteger> {
    @Override
    public void coGroup(PactString key, Iterator<PactString> values1, Iterator<PactString> values2,
        Collector<PactString, PactInteger> out) {
      //IMPLEMENT ME
    }
  }

  @Override
  public Plan getPlan(String... args) throws IllegalArgumentException {
    //IMPLEMENT ME
    return null;
  }

  @Override
  public String getDescription() {
    return "Parameters: [noSubStasks] [input] [output]";
  }
}