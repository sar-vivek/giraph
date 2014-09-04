/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.giraph.examples;

import java.io.IOException;
import java.util.regex.Pattern;
import org.apache.giraph.examples.utils.LongPair;
import org.apache.giraph.io.EdgeReader;
import org.apache.giraph.io.formats.TextEdgeInputFormat;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * Simple text-based {@link org.apache.giraph.io.EdgeInputFormat} for
 * unweighted graphs with long ids.
 *
 * Each line consists of: source_vertex, target_vertex
 */
public class LongFloatTextEdgeInputFormat extends
    TextEdgeInputFormat<LongWritable, FloatWritable> {
  /** Splitter for endpoints */
  private static final Pattern SEPARATOR = Pattern.compile("[\t ]");

  @Override
  public EdgeReader<LongWritable, FloatWritable> createEdgeReader(
      InputSplit split, TaskAttemptContext context) throws IOException {
    return new LongFloatTextEdgeReader();
  }

  /**
   * {@link org.apache.giraph.io.EdgeReader} associated with
   * {@link LongFloatTextEdgeInputFormat}.
   */
  public class LongFloatTextEdgeReader extends
      TextEdgeReaderFromEachLineProcessed<LongPair> {
    @Override
    protected LongPair preprocessLine(Text line) throws IOException {
      String[] tokens = SEPARATOR.split(line.toString());
      return new LongPair(Long.valueOf(tokens[0]),
          Long.valueOf(tokens[1]));
    }

    @Override
    protected LongWritable getSourceVertexId(LongPair endpoints)
      throws IOException {
      return new LongWritable(endpoints.getFirst());
    }

    @Override
    protected LongWritable getTargetVertexId(LongPair endpoints)
      throws IOException {
      return new LongWritable(endpoints.getSecond());
    }

    @Override
    protected FloatWritable getValue(LongPair endpoints) throws IOException {
      return new FloatWritable(1f);
    }
  }
}
