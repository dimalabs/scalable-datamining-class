package de.tuberlin.dima.aim3.assignment3;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/** DONT USE THIS CLASS */
public class ReplaceMeWritable implements WritableComparable<ReplaceMeWritable> {

  @Override
  public int compareTo(ReplaceMeWritable o) {
    throw new RuntimeException("Dont use this class");
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    throw new RuntimeException("Dont use this class");
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    throw new RuntimeException("Dont use this class");
  }
}
