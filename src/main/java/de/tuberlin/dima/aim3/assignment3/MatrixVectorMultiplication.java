package de.tuberlin.dima.aim3.assignment3;

import de.tuberlin.dima.aim3.HadoopJob;
import org.apache.hadoop.fs.Path;

import java.util.Map;

public class MatrixVectorMultiplication extends HadoopJob {

  @Override
  public int run(String[] args) throws Exception {

    Map<String,String> parsedArgs = parseArgs(args);

    Path matrix = new Path(parsedArgs.get("--matrix"));
    Path vector = new Path(parsedArgs.get("--vector"));
    Path outputPath = new Path(parsedArgs.get("--output"));

    //IMPLEMENT ME

    return 0;
  }
}
