/*
 * Copyright 2014 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.giraph.examples;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

/**
 *
 * @author vsardeshmukh
 */
public class DiseaseWritable implements Writable {
     /**
     * Minimum distance to source node
     */
    private double dist;
    /**
     * probability that it will transmit a disease
     */
    private double prob;
    
     
    public DiseaseWritable(){
    
    }
    
    public DiseaseWritable(double p, double d){
        this.dist = d;
        this.prob = p;
    }
    
    public double getDist() { return this.dist; }
    
    public double getProb() { return this.prob; }
    
    
    public void setDist(double d){
        this.dist = d;
    }
    
    public void setProb(double p){
        this.prob = p;
    }
   
    @Override
    public void readFields(DataInput input) throws IOException {
        dist = input.readDouble();
        prob = input.readDouble();
    }
    @Override
    public void write(DataOutput output) throws IOException {
        output.writeDouble(dist);
        output.writeDouble(prob);
    } 
    @Override
    public String toString() {
       // return "(dist = " + dist + ", bucket = " + bucket + ", flag = " + 
        //        doneLight + ")";
        return  Double.toString(dist);
    }
}
