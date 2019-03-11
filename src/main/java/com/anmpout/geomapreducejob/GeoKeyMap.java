/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.anmpout.geomapreducejob;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.*;

/**
 *
 * @author cloudera
 */
public class GeoKeyMap implements WritableComparable{
    private GeoKey key;
    private LongWritable timeslot;

    public GeoKeyMap(GeoKey key, LongWritable timeslot) {
        this.key = key;
        this.timeslot = timeslot;
    }

    public GeoKeyMap() {
        key= null;
        timeslot = null;
    }
    

    public GeoKey getKey() {
        return key;
    }

    public void setKey(GeoKey key) {
        this.key = key;
    }

    public LongWritable getTimeslot() {
        return timeslot;
    }

    public void setTimeslot(LongWritable timeslot) {
        this.timeslot = timeslot;
    }
    @Override
public void write(DataOutput d) throws IOException {
key.write(d);
timeslot.write(d);

}

@Override
public void readFields(DataInput di) throws IOException {

if (key == null) {
key = new GeoKey();
}
if (timeslot == null) {
timeslot = new LongWritable();
}

key.readFields(di);
timeslot.readFields(di);
    
}

@Override
public int compareTo(Object o) {
GeoKeyMap other = (GeoKeyMap)o;
int cmp = key.compareTo(other.key);
if (cmp != 0) {
return cmp;
}
return timeslot.compareTo(other.timeslot);
    
    
}
    
    
    
}
