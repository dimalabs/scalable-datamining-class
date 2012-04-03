package de.tuberlin.dima.aim3.assignment2;

import de.tuberlin.dima.aim3.HadoopJob;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.mahout.math.VectorWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;


public class MatrixTransposition extends HadoopJob {

  @Override
  public int run(String[] args) throws Exception {

    Map<String,String> parsedArgs = parseArgs(args);

    Path inputPath = new Path(parsedArgs.get("--input"));
    Path outputPath = new Path(parsedArgs.get("--output"));

    Job transpose = prepareJob(inputPath, outputPath, SequenceFileInputFormat.class, MatrixTranspositionMapper.class,
        IntWritable.class, VectorElementWritable.class, MatrixTranspositionReducer.class, IntWritable.class,
        VectorWritable.class, SequenceFileOutputFormat.class);
    transpose.waitForCompletion(true);
    
    return 0;
  }
  
  static class VectorElementWritable implements Writable {

    @Override
    public void write(DataOutput out) throws IOException {
      // IMPLEMENT ME
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      // IMPLEMENT ME
    }

    // IMPLEMENT equals/hashCode
  }

  static class MatrixTranspositionMapper extends Mapper<IntWritable,VectorWritable,IntWritable,VectorElementWritable> {
    @Override
    protected void map(IntWritable rowID, VectorWritable row, Context ctx) throws IOException, InterruptedException {
      // IMPLEMENT ME
    }
  }

  static class MatrixTranspositionReducer
      extends Reducer<IntWritable,VectorElementWritable,IntWritable,VectorWritable> {
    @Override
    protected void reduce(IntWritable rowID, Iterable<VectorElementWritable> elements, Context ctx)
        throws IOException, InterruptedException {
      // IMPLEMENT ME
    }
  }
  
}
