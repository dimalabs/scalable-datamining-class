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
            return true;
        }
    }

    public static class CoGroupOutFormat extends TextOutputFormat<PactString, PactInteger> {
        @Override
        public byte[] writeLine(KeyValuePair<PactString, PactInteger> pair) {
            return null;
        }
    }

    public static class TestCoGrouper extends CoGroupStub<PactString, PactString, PactString, PactString, PactInteger> {
        @Override
        public void coGroup(PactString key, Iterator<PactString> values1, Iterator<PactString> values2,
                Collector<PactString, PactInteger> out) {
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