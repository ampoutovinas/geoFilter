/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.anmpout.geomapreducejob;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author cloudera
 */
  public class GeoFilterMapper extends Mapper<GeoKey,
GeoValue, GeoKeyMap, GeoValue> {
@Override
protected void map(GeoKey key, GeoValue value, Mapper.Context
context) throws IOException, InterruptedException {
    long bucket = 0;
    if(key.getLocation() != null){
String location = key.getLocation().toString();
            bucket =  value.getUnixTimestamp().get() - (value.getUnixTimestamp().get() % 3600);
GeoKeyMap mapKey = new GeoKeyMap(key,new LongWritable(bucket));

context.write(mapKey,value);

}
}
}
