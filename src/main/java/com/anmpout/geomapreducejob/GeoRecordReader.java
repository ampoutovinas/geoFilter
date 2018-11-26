/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.anmpout.geomapreducejob;

import com.spatial4j.core.context.SpatialContext;
import com.spatial4j.core.shape.*;
import com.spatial4j.core.shape.impl.*;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;

/**
 *
 * @author cloudera
 */
public class GeoRecordReader extends RecordReader<GeoKey,GeoValue> {
private static final Logger LOG = Logger.getLogger(GeoRecordReader.class); 

private GeoKey key;
private GeoValue value;
private  LineRecordReader reader = new LineRecordReader();
 List<MyPath> myPaths = new ArrayList<MyPath>();

    public GeoRecordReader() {
    
    }

    @Override
    public void initialize(InputSplit is, TaskAttemptContext tac) throws IOException, InterruptedException {

        reader.initialize(is, tac);
        try {
        String data = readFileAsString("/home/cloudera/Downloads/paths");
        myPaths = MyPath.listfromJSON(data);
    } catch (Exception ex) {
        LOG.debug(ex.toString());
    }
               
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
boolean gotNextKeyValue = reader.nextKeyValue();
if(gotNextKeyValue) {
if (key == null) {
key = new GeoKey();
}
if (value == null) {
value = new GeoValue();
}

Text line = reader.getCurrentValue();
LOG.debug(line.toString());
String[] tokens = line.toString().split("\t");




 PointImpl tmp = new PointImpl(Double.
parseDouble(tokens[1]),Double.
parseDouble(tokens[2]), SpatialContext.GEO);
 for(MyPath path:myPaths){
 SpatialRelation relation = path.getPolyline().relate(tmp);
 if(relation.equals(SpatialRelation.CONTAINS)){
 key.setLocation(new Text(path.getPathId()));
 value.setActor(new Text(path.getPathId()));
 }else{
 key.setLocation(new Text("kaiadas"));
 value.setActor(new Text("kaiadas"));
 }
 }



value.setEventDate(new Text(tokens[1]));
value.setEventType(new Text(tokens[2]));
try {
value.setFatalities(new IntWritable(Integer.
parseInt(tokens[3])));
} catch(NumberFormatException ex) {
value.setFatalities(new IntWritable(0));
}
value.setSource(new Text(tokens[2]));
}
else {
key = null;
value = null;
}
return gotNextKeyValue;
    }

    @Override
    public GeoKey getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public GeoValue getCurrentValue() throws IOException, InterruptedException {
    return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return reader.getProgress();
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }
    
        public static String readFileAsString(String fileName)throws Exception 
  { 
    String data = ""; 
    data = new String(Files.readAllBytes(Paths.get(fileName))); 
    return data; 
  } 
    
    
}
