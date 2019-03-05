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
public class GeoFilterReducer extends Reducer<GeoKey,
GeoValue, Text, Text> {

    @Override
    protected void reduce(GeoKey key, java.lang.Iterable<GeoValue> values, Context context) throws IOException, InterruptedException {
       double speed = 0;
        int count = 0;
        int time = 0;
        String timeString ="";
       for (GeoValue value : values) {
           if(value.getSpeed().get()>0){
        speed = value.getSpeed().get() + speed;
        count = count+1;
           }
      }
            context.write(key.getLocation(), new Text(String.valueOf(speed)+"\n"));
       speed= speed/count;
       if(speed != 0){
       time = (int) (key.getDistance().get()/speed);
       }
      double hours = time / 3600;
      double minutes = (time % 3600) / 60;
      double seconds = time % 60;

//timeString = String.format("%02d:%02d:%02d", hours, minutes, seconds);
       

     context.write(key.getLocation(), new Text(String.valueOf(count)+"\n"));
      context.write(key.getLocation(), new Text(String.valueOf(speed)+"\n"));
       context.write(key.getLocation(), new Text(String.valueOf(time)+"\n")); 
       context.write(key.getLocation(), new Text(String.valueOf(key.getDistance().get())+"\n\n"));  
    }
    
    
    
    
}
