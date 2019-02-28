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
public class GeoKey implements WritableComparable{

private Text location;
private DoubleWritable latitude;
private DoubleWritable longitude;
public GeoKey() {
location = null;
latitude = null;
longitude = null;
}

public GeoKey(Text location, DoubleWritable latitude,
DoubleWritable longitude) {
this.location = location;
this.latitude = latitude;
this.longitude = longitude;
}



@Override
public void write(DataOutput d) throws IOException {
throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
}

@Override
public void readFields(DataInput di) throws IOException {

if (location == null) {
location = new Text();
}
if (latitude == null) {
latitude = new DoubleWritable();
}
if (longitude == null) {
longitude = new DoubleWritable();
}
location.readFields(di);
latitude.readFields(di);
longitude.readFields(di);    
    
}

@Override
public int compareTo(Object o) {
GeoKey other = (GeoKey)o;
int cmp = location.compareTo(other.location);
if (cmp != 0) {
return cmp;
}
cmp = latitude.compareTo(other.latitude);
if (cmp != 0) {
return cmp;
}
return longitude.compareTo(other.longitude);
    
    
}

    public Text getLocation() {
        return location;
    }

    public void setLocation(Text location) {
        this.location = location;
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

    
    

}
