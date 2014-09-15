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

import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.regex.Pattern;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.io.formats.TextVertexInputFormat;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * Simple text-based {@link org.apache.giraph.io.VertexInputFormat} for
 * unweighted graphs with int ids.
 *
 * Each line consists of: vertex_id vertex_value 
 */
public class LongDiseaseFloatVertexInputFormat
    extends
    TextVertexInputFormat<LongWritable, DiseaseWritable, FloatWritable> {
  /** Separator of the vertex and neighbors */
  private static final Pattern SEPARATOR = Pattern.compile("[\t ]");

  @Override
  public TextVertexReader createVertexReader(InputSplit split,
      TaskAttemptContext context)
    throws IOException {
    return new LongDiseaseFloatVertexReader();
  }

  /**
   * Vertex reader associated with
   * {@link org.apache.giraph.examples.LongDoubleFloatVertexInputFormat}
   */
  public class LongDiseaseFloatVertexReader extends
    TextVertexReaderFromEachLineProcessed<String[]> {
    /** Cached vertex id for the current line */
    private LongWritable id;
    /** Cached vertex value for the current line */
    private DiseaseWritable value;

    @Override
    protected String[] preprocessLine(Text line) throws IOException {
      String[] tokens = SEPARATOR.split(line.toString());
      id = new LongWritable(Long.parseLong(tokens[0]));
      value = new DiseaseWritable(Double.parseDouble(tokens[1]), Double.MAX_VALUE);
      return tokens;
    }

    @Override
    protected LongWritable getId(String[] tokens) throws IOException {
      return id;
    }

    @Override
    protected DiseaseWritable getValue(String[] tokens) throws IOException {
      return value;
    }
		
    @Override
    protected Iterable<Edge<LongWritable, FloatWritable>> getEdges(String[] tokens)
      throws IOException {
      return ImmutableList.of();
    }
  }
}
