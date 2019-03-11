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
private Text timestamp;
private DoubleWritable latitude;
private DoubleWritable longitude;
private DoubleWritable altitude;
private DoubleWritable speed;
private DoubleWritable orientation;
private LongWritable unixTimestamp;

    public GeoValue() {
         this.timestamp = null;
        this.latitude = null;
        this.longitude = null;
        this.altitude = null;
        this.speed = null;
        this.orientation = null;
        this.unixTimestamp = null;
    }

    public GeoValue(Text timestamp, DoubleWritable latitude, DoubleWritable longitude,
            DoubleWritable altitude, DoubleWritable speed,DoubleWritable orientation,LongWritable unixTimestamp) {
        this.timestamp = timestamp;
        this.latitude = latitude;
        this.longitude = longitude;
        this.altitude = altitude;
        this.speed = speed;
        this.orientation = orientation;
        this.unixTimestamp = unixTimestamp;
    }

    public Text getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Text timestamp) {
        this.timestamp = timestamp;
    }

    public DoubleWritable getLatitude() {
        return latitude;
    }

    public void setLatitude(DoubleWritable latitude) {
        this.latitude = latitude;
    }

    public DoubleWritable getLongitude() {
        return longitude;
    }

    public void setLongitude(DoubleWritable longitude) {
        this.longitude = longitude;
    }

    public DoubleWritable getAltitude() {
        return altitude;
    }

    public void setAltitude(DoubleWritable altitude) {
        this.altitude = altitude;
    }

    public DoubleWritable getSpeed() {
        return speed;
    }

    public void setSpeed(DoubleWritable speed) {
        this.speed = speed;
    }

    public DoubleWritable getOrientation() {
        return orientation;
    }

    public void setOrientation(DoubleWritable orientation) {
        this.orientation = orientation;
    }

    public LongWritable getUnixTimestamp() {
        return unixTimestamp;
    }

    public void setUnixTimestamp(LongWritable unixTimestamp) {
        this.unixTimestamp = unixTimestamp;
    }
    
    


    @Override
    public void write(DataOutput d) throws IOException {
       timestamp.write(d);
       longitude.write(d);
       latitude.write(d);
       altitude.write(d);
       speed.write(d);
       orientation.write(d);
       unixTimestamp.write(d);
       
      
      

    }

    @Override
    public void readFields(DataInput di) throws IOException {
if (timestamp == null) {
timestamp = new Text();
}
if (latitude == null) {
latitude = new DoubleWritable();
}
if (longitude == null) {
longitude = new DoubleWritable();
}
if (altitude == null) {
altitude = new DoubleWritable();
}
if (speed == null) {
speed = new DoubleWritable();
}
if (orientation == null) {
orientation = new DoubleWritable();
}
if (unixTimestamp == null) {
unixTimestamp = new LongWritable();
}
timestamp.readFields(di);
latitude.readFields(di);
longitude.readFields(di);
altitude.readFields(di);
speed.readFields(di);
orientation.readFields(di);
unixTimestamp.readFields(di);
    
    
    }

    @Override
    public int compareTo(Object o) {
GeoValue other = (GeoValue)o;
int cmp = timestamp.compareTo(other.timestamp);
if (cmp != 0) {
return cmp;
}
cmp = latitude.compareTo(other.latitude);
if (cmp != 0) {
return cmp;
}
cmp = longitude.compareTo(other.longitude);
if (cmp != 0) {
return cmp;
}
cmp = altitude.compareTo(other.altitude);
if (cmp != 0) {
return cmp;
}
cmp = speed.compareTo(other.speed);
if (cmp != 0) {
return cmp;
}
cmp = unixTimestamp.compareTo(other.unixTimestamp);
if (cmp != 0) {
return cmp;
}
return orientation.compareTo(other.orientation);
    }


}
