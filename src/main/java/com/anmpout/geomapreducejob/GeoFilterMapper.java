/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.anmpout.geomapreducejob;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author cloudera
 */
  public class GeoFilterMapper extends Mapper<GeoKey,
GeoValue, Text, Text> {
@Override
protected void map(GeoKey key, GeoValue value, Mapper.Context
context) throws IOException, InterruptedException {
    if(key.getLocation() != null){
String location = key.getLocation().toString();
//if (location.toLowerCase().equals("12")||location.toLowerCase().equals("1")
//        ||location.toLowerCase().equals("8")  ||location.toLowerCase().equals("7") ||
//        location.toLowerCase().equals("189")||location.toLowerCase().equals("165")  ) {
context.write(key.getLocation(),new Text("mapper"));
//}
}
}
}
