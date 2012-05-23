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

package de.tuberlin.dima.aim3.assignment3;

import com.google.common.base.Preconditions;
import com.google.common.io.Closeables;
import de.tuberlin.dima.aim3.HadoopAndPactTestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertEquals;

/**
 * Unit test for search as matrix vector multiplication
 */
public class SearchAsMatrixVectorMultiplicationTest extends HadoopAndPactTestCase {

  @Test
  public void search() throws Exception {

	File corpusAsMatrix = getTestTempFile("corpusAsMatrix");
    File inputFile = getTestTempFile("lotr.txt");
    File queryFile = getTestTempFile("query.txt");
    File outputDir = getTestTempDir("output");
    corpusAsMatrix.delete();
    outputDir.delete();

    writeLines(inputFile,
        "0;One Ring to rule them all,",
        "1;One Ring to find them,",
        "2;One Ring to bring them all",
        "3;and in the darkness bind them");

    writeLines(queryFile, "ring and darkness them");

    Configuration conf = new Configuration();
    SearchAsMatrixVectorMultiplication search = new SearchAsMatrixVectorMultiplication();
    search.setConf(conf);

    search.run(new String[] { "--input", inputFile.getAbsolutePath(),
    		"--query", queryFile.getAbsolutePath(),
    		"--output", outputDir.getAbsolutePath(),
    		"--matrix", corpusAsMatrix.getAbsolutePath() });

    SparseVector result = readResult(new File(outputDir, "part-r-00000"), conf);

    assertEquals(2, result.get(0), 0);
    assertEquals(2, result.get(1), 0);
    assertEquals(2, result.get(2), 0);
    assertEquals(3, result.get(3), 0);
  }

  protected SparseVector readResult(File outputFile, Configuration conf) throws IOException {
    SequenceFile.Reader reader = null;
    try {
      reader = new SequenceFile.Reader(FileSystem.get(conf), new Path(outputFile.getAbsolutePath()), conf);

      Writable row = NullWritable.get();
      SparseVector vector = new SparseVector();

      Preconditions.checkArgument(reader.getValueClass().equals(SparseVector.class),
          "value type of sequencefile must be a SparseVector");

      boolean hasAtLeastOneRow = reader.next(row, vector);
      Preconditions.checkState(hasAtLeastOneRow, "result must have at least one value");

      return vector;

    } finally {
      Closeables.closeQuietly(reader);
    }

  }
}
