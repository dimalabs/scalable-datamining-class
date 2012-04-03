package de.tuberlin.dima.aim3.assignment1;

import com.google.common.collect.Maps;
import de.tuberlin.dima.aim3.HadoopAndPactTestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.mahout.common.iterator.FileLineIterable;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;

public class AverageTemperaturePerMonthTest extends HadoopAndPactTestCase {

  @Test
  public void countWords() throws Exception {

    File inputFile = getTestTempFile("temperatures.tsv");
    File outputDir = getTestTempDir("output");
    outputDir.delete();

    writeLines(inputFile, readLines("/assignment1/temperatures.tsv"));

    double minimumQuality = 0.25;

    Configuration conf = new Configuration();
    AverageTemperaturePerMonth averageTemperaturePerMonth = new AverageTemperaturePerMonth();
    averageTemperaturePerMonth.setConf(conf);

    averageTemperaturePerMonth.run(new String[] { "--input", inputFile.getAbsolutePath(),
        "--output", outputDir.getAbsolutePath(), "--minimumQuality", String.valueOf(minimumQuality) });


    Map<YearAndMonth, Double> results = readResults(new File(outputDir, "part-r-00000"));

    assertEquals(results.get(new YearAndMonth(1990, 8)), 8, EPSILON);
    assertEquals(results.get(new YearAndMonth(1992, 4)), 7.888d, EPSILON);
    assertEquals(results.get(new YearAndMonth(1994, 1)), 8.24, EPSILON);
  }


  class YearAndMonth {

    private final int year;
    private final int month;

    public YearAndMonth(int year, int month) {
      this.year = year;
      this.month = month;
    }

    @Override
    public boolean equals(Object o) {
      if (o instanceof YearAndMonth) {
        YearAndMonth other = (YearAndMonth) o;
        return year == other.year && month == other.month;
      }
      return false;
    }

    @Override
    public int hashCode() {
      return 31 * year + month;
    }
  }

  private Map<YearAndMonth,Double> readResults(File outputFile) throws IOException {
    Pattern separator = Pattern.compile("\t");
    Map<YearAndMonth,Double> averageTemperatures = Maps.newHashMap();
    for (String line : new FileLineIterable(outputFile)) {
      String[] tokens = separator.split(line);
      int year = Integer.parseInt(tokens[0]);
      int month = Integer.parseInt(tokens[1]);
      double temperature = Double.parseDouble(tokens[2]);
      averageTemperatures.put(new YearAndMonth(year, month), temperature);
    }
    return averageTemperatures;
  }

}
