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
Text, Text, Text> {

    @Override
    protected void reduce(Text key, java.lang.Iterable<Text> values, Context context) throws IOException, InterruptedException {
       int sum = 0;
       for (Text count : values) {
        sum += 1;
      }
      context.write(key, new Text(String.valueOf(sum)));
        
    }
    
    
    
    
}
