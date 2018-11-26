/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.anmpout.geomapreducejob;

import java.util.Calendar;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

/**
 *
 * @author cloudera
 */
public class GeoFilter extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(GeoFilter.class);  
  


    @Override
    public int run(String[] args) throws Exception {
                 Job job = Job.getInstance(getConf(), "GeoFilter");
         job.setJarByClass(this.getClass());
                
        FileInputFormat.addInputPath(job,new Path("/user/thesis/samples/2_hours"));
        FileOutputFormat.setOutputPath(job,new Path("/user/thesis/sample1/output/"
                +Long.toString(Calendar.getInstance().getTimeInMillis())));
   // FileInputFormat.addInputPath(job, new Path(args[0]));
   // FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(GeoFilterMapper.class);
job.setMapOutputKeyClass(Text.class);
job.setMapOutputValueClass(IntWritable.class);
job.setInputFormatClass(GeoInputFormat.class);
job.setOutputFormatClass(TextOutputFormat.class);
    return job.waitForCompletion(true) ? 0 : 1;
        




    }
  public static void main(String[] args) throws Exception {
int exitCode = ToolRunner.run(new GeoFilter(), args);
System.exit(exitCode);
  
  } 
    
}
  

