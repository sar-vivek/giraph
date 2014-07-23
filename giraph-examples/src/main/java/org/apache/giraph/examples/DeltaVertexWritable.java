/**
 *
 * @author Vivek B Sardeshmukh
 */

package org.apache.giraph.examples;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

public class DeltaVertexWritable implements Writable {
    /**
     * Minimum distance to source node
     */
    private double dist;
    /**
     * bucket index of the vertex
     */
    private long bucket;
    /**
     * 0 - if I haven't done anything since I fall in this bucket
     * 1 - I processed light edges when I was in this bucket
     *     need to process heavy when all guys in this bucket are done
     * 2 - I processed heavy edges when I was in this bucket
     *     nothing to do, I'll do something if I upgraded to different bucket 
     *     (by setting this value to 0 again).
     */
    private int doneLight;
    
    public DeltaVertexWritable(){
    
    }
    
    public DeltaVertexWritable(double d, long b, int f){
        this.dist = d;
        this.bucket = b;
        this.doneLight = f;
    }
    
    public double getDist() { return this.dist; }
    
    public long getBucket() { return this.bucket; }
    
    public boolean isProcessing() { return this.doneLight==0?true:false; }
    
    public boolean isLightDone() { return this.doneLight==1?true:false; }
    
    public boolean isHeavyDone() { return this.doneLight==2?true:false; }
    
    public void setDist(double d){
        this.dist = d;
    }
    
    public void setBucket(long b){
        this.bucket = b;
    }
    
    public void setDoneLight(int f){
        this.doneLight = f;
    }
    
    @Override
    public void readFields(DataInput input) throws IOException {
        dist = input.readDouble();
        bucket = input.readLong();
        doneLight = input.readInt();
    }
    @Override
    public void write(DataOutput output) throws IOException {
        output.writeDouble(dist);
        output.writeLong(bucket);
        output.writeInt(doneLight);
    } 
    @Override
    public String toString() {
       // return "(dist = " + dist + ", bucket = " + bucket + ", flag = " + 
        //        doneLight + ")";
        return  Double.toString(dist);
    }
}
