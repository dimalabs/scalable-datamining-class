package de.tuberlin.dima.aim3.assignment3;

import de.tuberlin.dima.aim3.HadoopJob;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;
import java.util.Map;

public class SearchAsMatrixVectorMultiplication extends HadoopJob {

  @Override
  public int run(String[] args) throws Exception {

    Map<String,String> parsedArgs = parseArgs(args);

    Path inputPath = new Path(parsedArgs.get("--input"));
    Path queryPath = new Path(parsedArgs.get("--query"));
    Path corpusAsMatrix = new Path(parsedArgs.get("--matrix"));

    Job vectorize = prepareJob(inputPath, corpusAsMatrix, TextInputFormat.class, VectorizeSentencesMapper.class,
        IntWritable.class, SparseVector.class, SequenceFileOutputFormat.class);
    vectorize.waitForCompletion(true);

    //TODO execute the matrix vector multiplication. You can safely assume that the query-vector fits into the
    // main memory of the mapper instance

    return 0;
  }

  static class MatrixVectorMultiplicationMapper 
    extends Mapper<IntWritable, SparseVector, ReplaceMeWritable, ReplaceMeWritable> {

      //TODO IMPLEMENT ME
  }

  static class MatrixVectorMultiplicationReducer 
    extends Reducer<ReplaceMeWritable, ReplaceMeWritable, NullWritable, SparseVector> {
    //TODO IMPLEMENT ME
  }
}
