package de.tuberlin.dima.aim3.assignment3;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SparseVector implements Writable, Cloneable {

  //TODO IMPLEMENT ME

  public void set(int index, double value) {
    //TODO IMPLEMENT ME
  }

  public double dotProduct(SparseVector other) {
    //TODO IMPLEMENT ME
    return 0;
  }

  public double get(int index) {
    //TODO IMPLEMENT ME
    return 0;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    //TODO IMPLEMENT ME
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    //TODO IMPLEMENT ME
  }

  @Override
  public SparseVector clone() {
    //TODO IMPLEMENT ME
    return null;
  }
}
