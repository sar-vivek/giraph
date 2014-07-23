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
import org.apache.giraph.conf.LongConfOption;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.log4j.Logger;

/**
 * Demonstrates the Delta-Stepping Shortest Path Algorithm.
 *
 * @author Vivek B Sardeshmukh
 */
@Algorithm(
name = "Delta-Stepping Shortest paths",
description = "Finds all shortest paths from a selected vertex")
public class DeltaStepVertexComputation extends BasicComputation<
LongWritable, DeltaVertexWritable, FloatWritable, DoubleWritable> {

    /**
     * The shortest paths id
     */
    public static final LongConfOption SOURCE_ID =
            new LongConfOption("DeltaStepVertex.sourceId", 1,
            "The shortest paths id");
    public static final LongConfOption DELTA =
            new LongConfOption("DeltaStepVertex.delta", 1,
            "Delta value");
    /**
     *
     * Class logger
     */
    private static final Logger LOG =
            Logger.getLogger(DeltaStepVertexComputation.class);
    /**
     * Name of aggregator for the active bucket_index
     */
    static final String BUCKET_INDEX = 
            DeltaStepVertexComputation.class.getName() + ".bucketIndexAgg";
    public long doneSuperStep;

    /**
     * Is this vertex the source id?
     *
     * @param vertex Vertex
     * @return True if the source id
     */
    private boolean isSource(Vertex<LongWritable, ?, ?> vertex) {
        return vertex.getId().get() == SOURCE_ID.get(getConf());
    }

    @Override
    public void compute(
            Vertex<LongWritable, DeltaVertexWritable, FloatWritable> vertex,
            Iterable<DoubleWritable> messages) throws IOException {

        long bucketIndex;
        DeltaVertexWritable dv;
        double minDist = isSource(vertex) ? 0d : Double.MAX_VALUE;

        if (getSuperstep() == 0) {
            bucketIndex = isSource(vertex) ? 0l : Long.MAX_VALUE;
            vertex.setValue(new DeltaVertexWritable(minDist, bucketIndex, 0));
            if (isSource(vertex)) {
                for (Edge<LongWritable, FloatWritable> edge : vertex.getEdges()) {
                    if (edge.getValue().get() <= DELTA.get(getConf())) {
                        double distance = vertex.getValue().getDist()
                                + edge.getValue().get();
                        sendMessage(edge.getTargetVertexId(),
                                new DoubleWritable(distance));
                    }
                }
            }
            aggregate(BUCKET_INDEX, new LongWritable(bucketIndex));
        }


        /* receive messages - 
         * - I'm receiving messages because some neighbor called relax(v,x) 
         * - Update my tent value and bucket index
         * - broadcast bucket index so that in the next step folks can compute 
         *   which bucket to process
         */
        for (DoubleWritable message : messages) {
            minDist = Math.min(minDist, message.get());
        }
        if (minDist < vertex.getValue().getDist()) {   /*relax procedure*/
            bucketIndex = (long) ((long) minDist / DELTA.get(getConf()));
            vertex.setValue(new DeltaVertexWritable(minDist, bucketIndex, 0));
            //  aggregate(BUCKET_INDEX, new LongWritable(bucketIndex));
        }

        if (getSuperstep() > 0) {

            long minBucketIndex =
                    ((LongWritable) getAggregatedValue(BUCKET_INDEX)).get();

            if (vertex.getValue().getBucket() == minBucketIndex) {
                /*I belong to the bucket which is we are going to process*/
                for (Edge<LongWritable, FloatWritable> edge : vertex.getEdges()) {
                    /*light edges*/
                    if (edge.getValue().get() <= DELTA.get(getConf())) {
                        double distance = vertex.getValue().getDist()
                                + edge.getValue().get();
                        sendMessage(edge.getTargetVertexId(),
                                new DoubleWritable(distance));
                    }
                }
                dv = vertex.getValue();
                dv.setDoneLight(1);
                vertex.setValue(dv);
            }             
            else if ((minBucketIndex > vertex.getValue().getBucket()) && 
                    (vertex.getValue().isLightDone())) {
                for (Edge<LongWritable, FloatWritable> edge : vertex.getEdges()) {
                    if (edge.getValue().get() > DELTA.get(getConf())) {
                        double distance = vertex.getValue().getDist()
                                + edge.getValue().get();
                        sendMessage(edge.getTargetVertexId(),
                                new DoubleWritable(distance));
                    }
                }
                dv = vertex.getValue();
                dv.setDoneLight(2);
                vertex.setValue(dv);
            }
        }

        //		if (LOG.isDebugEnabled()) {
        LOG.debug("Superstep " + getSuperstep() + " : " 
                + vertex.getValue().toString());
        //		}

        if (vertex.getValue().isHeavyDone()) {
            vertex.voteToHalt();
            return;
        } else if (vertex.getValue().isProcessing()){
            aggregate(BUCKET_INDEX,
                    new LongWritable(vertex.getValue().getBucket()));
            vertex.voteToHalt();
            return;
        }
    }
}
