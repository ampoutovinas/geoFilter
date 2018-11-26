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
public class GeoValue implements WritableComparable {
private Text eventDate;
private Text eventType;
private Text actor;
private Text source;
private IntWritable fatalities; 

    public GeoValue() {
        this.eventDate = null;
        this.eventType = null;
        this.actor = null;
        this.source = null;
        this.fatalities = null;
    }

    public GeoValue(Text eventDate, Text eventType, Text actor, Text source, IntWritable fatalities) {
        this.eventDate = eventDate;
        this.eventType = eventType;
        this.actor = actor;
        this.source = source;
        this.fatalities = fatalities;
    }

    public Text getEventDate() {
        return eventDate;
    }

    public void setEventDate(Text eventDate) {
        this.eventDate = eventDate;
    }

    public Text getEventType() {
        return eventType;
    }

    public void setEventType(Text eventType) {
        this.eventType = eventType;
    }

    public Text getActor() {
        return actor;
    }

    public void setActor(Text actor) {
        this.actor = actor;
    }

    public Text getSource() {
        return source;
    }

    public void setSource(Text source) {
        this.source = source;
    }

    public IntWritable getFatalities() {
        return fatalities;
    }

    public void setFatalities(IntWritable fatalities) {
        this.fatalities = fatalities;
    }

    @Override
    public void write(DataOutput d) throws IOException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void readFields(DataInput di) throws IOException {
if (eventDate == null) {
eventDate = new Text();
}
if (eventType == null) {
eventType = new Text();
}
if (actor == null) {
actor = new Text();
}
if (source == null) {
source = new Text();
}
if (fatalities == null) {
fatalities = new IntWritable();
}
eventDate.readFields(di);
eventType.readFields(di);
actor.readFields(di);
source.readFields(di);
fatalities.readFields(di);
    
    
    }

    @Override
    public int compareTo(Object o) {
GeoValue other = (GeoValue)o;
int cmp = eventDate.compareTo(other.eventDate);
if (cmp != 0) {
return cmp;
}
cmp = eventType.compareTo(other.eventType);
if (cmp != 0) {
return cmp;
}
cmp = actor.compareTo(other.actor);
if (cmp != 0) {
return cmp;
}
cmp = source.compareTo(other.source);
if (cmp != 0) {
return cmp;
}
return fatalities.compareTo(other.fatalities);
    }


}
