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

import org.apache.giraph.aggregators.LongMinAggregator;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.log4j.Logger;

/**
 * Master compute associated with {@link DeltaStepComputation}. 
 * It regigsters aggregator. 
 * @author Vivek B Sardeshmukh
 */
public class DeltaStepVertexMasterCompute extends DefaultMasterCompute {


  /** logger */
  private static final Logger LOG =
      Logger.getLogger(DeltaStepVertexMasterCompute.class);

  @Override
  public void compute() {
  }

  @Override
  public void initialize() throws InstantiationException,
      IllegalAccessException {
    //registerAggregator(DeltaStepComputation.BUCKET_INDEX,
     //   LongMinAggregator.class);
    registerAggregator(DeltaStepVertexComputation.BUCKET_INDEX,
        LongMinAggregator.class);
  }
}
