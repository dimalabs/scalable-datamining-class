package de.tuberlin.dima.aim3.assignment2;

import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.MatchStub;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.common.type.base.PactLong;
import eu.stratosphere.pact.common.type.base.PactString;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class BookAndAuthorJoinPact implements PlanAssembler, PlanAssemblerDescription {

  @Override
  public String getDescription() {
    // IMPLEMENT ME
    return null;
  }

  @Override
  public Plan getPlan(String... args) throws IllegalArgumentException {
    // IMPLEMENT ME
    return null;
  }

  public static class BookAndAuthorMatch extends MatchStub<PactLong,PactString,BookAndYear,PactString,BookAndYear> {

    @Override
    public void match(PactLong authorID, PactString authorName, BookAndYear bookAndYear,
        Collector<PactString, BookAndYear> collector) {
      // IMPLEMENT ME
    }
  }

  public static class BookAndYear implements Value {

    public BookAndYear() {}

    public BookAndYear(String title, short year) {
      // IMPLEMENT
    }

    @Override
    public void write(DataOutput out) throws IOException {
      // IMPLEMENT
    }

    @Override
    public void read(DataInput out) throws IOException {
      // IMPLEMENT
    }

    // IMPLEMENT equals/hashCode
  }
}