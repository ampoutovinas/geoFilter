/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.anmpout.geomapreducejob;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import org.apache.hadoop.io.DoubleWritable;
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
        List<Double> allSpeeds = new ArrayList<>();
        
      
       for (GeoValue value : values) {
           if(value.getSpeed().get()>0){
        speed = value.getSpeed().get() + speed;
        count = count+1;
        allSpeeds.add(value.getSpeed().get());
           }
      }
        Double max = max(allSpeeds);
        Double min = min(allSpeeds);
        Double median = getMedian(allSpeeds);
        
        
       
       speed= speed/count;
       if(speed != 0){
       time = (int) (keyMap.getKey().getDistance().get()/speed);
       }
//      double hours = time / 3600;
//      double minutes = (time % 3600) / 60;
//      double seconds = time % 60;
     long timestamp = keyMap.getTimeslot().get()*1000;
    Date d = new Date(timestamp);
    Calendar cal = Calendar.getInstance();
    cal.setTime(d);
    int year = cal.get(Calendar.YEAR);
    int month = cal.get(Calendar.MONTH)+1;
    int day = cal.get(Calendar.DAY_OF_MONTH);
        
//timeString = String.format("%02d:%02d:%02d", hours, minutes, seconds);

     context.write(keyMap.getKey().getLocation(),new Text(String.valueOf(keyMap.getTimeslot())+"\t"+String.valueOf(count)+"\t"+
             String.valueOf(speed)+"\t"+String.valueOf(time)+"\t"+String.valueOf(keyMap.getKey().getDistance().get())+"\t"+
             String.valueOf(max)+"\t"+String.valueOf(min)+"\t"+String.valueOf(median)+"\t"+
             String.valueOf(day)+"\t"+String.valueOf(month)+"\t"+String.valueOf(year)));
//      context.write(key.getLocation(), new Text());
//       context.write(key.getLocation(), new Text()); 
//       context.write(key.getLocation(), new Text()+"\n\n"));  
    }
    
       public Double min(List<Double> array) {
     if(array.isEmpty()) return 0.0;
      Double min = array.get(0);
     
      for(int i=0; i<array.size(); i++) {
         if(array.get(i).compareTo(min)<0) {
            min = array.get(i);
         }
      }
      return min;
   }
    public Double getMedian(List<Double> array) {
    if(array.isEmpty()) return  0.0;
    Collections.sort(array);
    int middle = array.size() / 2;
    middle = middle > 0 && middle % 2 == 0 ? middle - 1 : middle;
    return array.get(middle);
}
      public Double max(List<Double> array) {
      if(array.isEmpty()) return 0.0;
      Double max = array.get(0);
     
      for(int i=0; i<array.size(); i++) {
         if(array.get(i).compareTo(max)>0) {
            max = array.get(i);
         }
      }
      return max;
   }
}
