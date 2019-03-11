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
public class GeoFilterReducer extends Reducer<GeoKeyMap,
GeoValue, Text, Text> {

    @Override
    protected void reduce(GeoKeyMap keyMap, java.lang.Iterable<GeoValue> values, Context context) throws IOException, InterruptedException {
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
       speed= speed/count;
       if(speed != 0){
       time = (int) (keyMap.getKey().getDistance().get()/speed);
       }
      double hours = time / 3600;
      double minutes = (time % 3600) / 60;
      double seconds = time % 60;

//timeString = String.format("%02d:%02d:%02d", hours, minutes, seconds);

     context.write(keyMap.getKey().getLocation(),new Text(String.valueOf(keyMap.getTimeslot())+"\t"+String.valueOf(count)+"\t"+
             String.valueOf(speed)+"\t"+String.valueOf(time)+"\t"+String.valueOf(keyMap.getKey().getDistance().get())));
//      context.write(key.getLocation(), new Text());
//       context.write(key.getLocation(), new Text()); 
//       context.write(key.getLocation(), new Text()+"\n\n"));  
    }
    
    
    
    
}
