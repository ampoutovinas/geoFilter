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
GeoValue, Text, IntWritable> {
@Override
protected void map(GeoKey key, GeoValue value, Mapper.Context
context) throws IOException, InterruptedException {
String location = key.getLocation().toString();
if (!location.toLowerCase().equals("kaiadas")) {
context.write(value.getActor(),new IntWritable(1));
}
}
}
