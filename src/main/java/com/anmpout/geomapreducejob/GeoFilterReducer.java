/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.anmpout.geomapreducejob;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author cloudera
 */
public class GeoFilterReducer extends Reducer<Text,
GeoValue, Text, Text> {

    @Override
    protected void reduce(Text key, java.lang.Iterable<GeoValue> values, Context context) throws IOException, InterruptedException {
       double speed = 0;
        int count = 0;
       for (GeoValue value : values) {
        speed = value.getSpeed().get() + speed;
        count = count+1;
      }
       speed= speed/count;
     context.write(key, new Text(String.valueOf(count)+"\n"));
      context.write(key, new Text(String.valueOf(speed)+"\n"));
        
    }
    
    
    
    
}
