package de.tuberlin.dima.aim3.assignment1;


import de.tuberlin.dima.aim3.HadoopJob;
import org.apache.hadoop.fs.Path;

import java.util.Map;

public class AverageTemperaturePerMonth extends HadoopJob {

  @Override
  public int run(String[] args) throws Exception {
    Map<String,String> parsedArgs = parseArgs(args);

    Path inputPath = new Path(parsedArgs.get("--input"));
    Path outputPath = new Path(parsedArgs.get("--output"));

    double minimumQuality = Double.parseDouble(parsedArgs.get("--minimumQuality"));

    //IMPLEMENT ME

    return 0;
  }
}