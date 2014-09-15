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
import java.util.Random;
import org.apache.giraph.conf.LongConfOption;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.MutableEdge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.log4j.Logger;

/**
 * Disease spread to Shortest path 
 */
@Algorithm(
    name = "Disease spread simulation",
    description = "Finds all shortest paths from a selected vertex"
)
public class DiseaseSpreadSimulation extends BasicComputation<
    LongWritable, DiseaseWritable, FloatWritable, DoubleWritable> {
  /** The shortest paths id */
  public static final LongConfOption SOURCE_ID =
      new LongConfOption("DiseaseSpreadSimulation.sourceId", 1,
          "seed for the disease");
  public static final LongConfOption SEED = 
      new LongConfOption("DiseaseSpreadSimulation.seed", 1, "seed for random");
  /** Class logger */
  private static final Logger LOG =
      Logger.getLogger(DiseaseSpreadSimulation.class);
  private Random rand = new Random();
  /**
   * Is this vertex the source id?
   *
   * @param vertex Vertex
   * @return True if the source id
   */
  private boolean isSource(Vertex<LongWritable, ?, ?> vertex) {
    return vertex.getId().get() == SOURCE_ID.get(getConf());
  }
  
  private float nextGeometric(double p){
      long geo = (long)Math.ceil(Math.log(rand.nextDouble())/Math.log(1-p));
      return (float)geo;
  }
  @Override
  public void compute(
      Vertex<LongWritable, DiseaseWritable, FloatWritable> vertex,
      Iterable<DoubleWritable> messages) throws IOException {
      double prob = vertex.getValue().getProb();
    if (getSuperstep() == 0) {
      vertex.setValue(new DiseaseWritable(prob, 
              Double.MAX_VALUE));
      for (MutableEdge<LongWritable, FloatWritable> edge : vertex.getMutableEdges()) {
          float distance = nextGeometric(prob);
          edge.setValue(new FloatWritable(distance));
          if (LOG.isDebugEnabled()) {
              LOG.debug("Vertex " + vertex.getId() + " to " +
                      edge.getTargetVertexId() + " = " + edge.getValue().get());
        }
      }
    }
    double minDist = isSource(vertex) ? 0d : Double.MAX_VALUE;
    for (DoubleWritable message : messages) {
      minDist = Math.min(minDist, message.get());
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Vertex " + vertex.getId() + " got minDist = " + minDist +
          " vertex value = " + vertex.getValue().getDist());
    }
    if (minDist < vertex.getValue().getDist()) {
      vertex.setValue(new DiseaseWritable(prob, minDist));
      for (Edge<LongWritable, FloatWritable> edge : vertex.getEdges()) {
        double distance = minDist + edge.getValue().get();
        if (LOG.isDebugEnabled()) {
          LOG.debug("Vertex " + vertex.getId() + " sent to " +
              edge.getTargetVertexId() + " = " + distance);
        }
        sendMessage(edge.getTargetVertexId(), new DoubleWritable(distance));
      }
    }
    vertex.voteToHalt();
  }
}
