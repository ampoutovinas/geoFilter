/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.anmpout.geomapreducejob;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Calendar;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
        String outputPath = "/user/thesis/sample1/output/"
                +Long.toString(Calendar.getInstance().getTimeInMillis());

                 Job job = Job.getInstance(getConf(), "GeoFilter");
         job.setJarByClass(this.getClass());
         try (Writer writer = new BufferedWriter(new OutputStreamWriter(
              new FileOutputStream("/home/cloudera/Downloads/mapperoutput.txt"), "utf-8"))) {
   writer.write(outputPath);
}       
        FileInputFormat.addInputPath(job,new Path("/user/thesis/samples/2_hours"));
        FileOutputFormat.setOutputPath(job,new Path(outputPath));
   // FileInputFormat.addInputPath(job, new Path(args[0]));
   // FileOutputFormat.setOutputPath(job, new Path(args[1]));
job.setMapperClass(GeoFilterMapper.class);
job.setMapOutputKeyClass(GeoKey.class);
job.setMapOutputValueClass(GeoValue.class);
job.setInputFormatClass(GeoInputFormat.class);
job.setReducerClass(GeoFilterReducer.class);
job.setOutputKeyClass(Text.class);
job.setOutputValueClass(Text.class);
job.setOutputFormatClass(TextOutputFormat.class);
        
if(job.waitForCompletion(true) && job.isSuccessful()){
             Configuration hadoopConfig = new Configuration();
             hadoopConfig.set("fs.defaultFS", "hdfs://quickstart.cloudera:8020");
            // hadoopConfig.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
             hadoopConfig.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
             FileSystem fs = FileSystem.get(hadoopConfig);
                Path inputPath2 = new Path(outputPath.trim().replace("\n","")+"/part-r-00000");
             openFileFromHDFS(fs,inputPath2);

}

    return job.waitForCompletion(true) ? 0 : 1;

    }
  public static void main(String[] args) throws Exception {
int exitCode = ToolRunner.run(new GeoFilter(), args);
System.exit(exitCode);
  
  } 
  
         private static void saveItemToDB(String readLine,Connection conn) {
        try{
     // the mysql insert statement
      String query = " insert into FILTER_DATA (PATH_ID, COUNT, SPEED, TIME, DISTANCE)"
        + " values (?, ?, ?, ?, ?)";

      // create the mysql insert preparedstatement
  
      String[] parts = readLine.split("\t");
      PreparedStatement preparedStmt = conn.prepareStatement(query);
      if(!parts[0].equals("") && !parts[0].equals("NaN")){
      preparedStmt.setInt (1, Integer.parseInt(parts[0]));
      }else{
                preparedStmt.setInt (1,0);
      }
        if(!parts[1].equals("") && !parts[1].equals("NaN")){
      preparedStmt.setInt (2, Integer.parseInt(parts[1]));
        }else{
               preparedStmt.setInt (2,0);
        }
          if(!parts[2].equals("") && !parts[2].equals("NaN")){
      preparedStmt.setDouble   (3, Double.parseDouble(parts[2]));
          }else{
                 preparedStmt.setDouble(3,0.0);
          }
            if(!parts[3].equals("") && !parts[3].equals("NaN")){
      preparedStmt.setInt(4, Integer.parseInt(parts[3]));
            }else{
               preparedStmt.setInt (4,0);
            }
              if(!parts[4].equals("") && !parts[4].equals("NaN")){
      preparedStmt.setDouble    (5, Double.parseDouble(parts[4]));
              }else{
                   preparedStmt.setDouble(5,0.0);
              }
     
      // execute the preparedstatement
      preparedStmt.execute();
      

    }
    catch (Exception e)
    {
      System.err.println(e.getMessage());

    }
        
        
    } 
         
               
        public static void openFileFromHDFS(FileSystem fs,Path path) throws IOException, ClassNotFoundException, InstantiationException, IllegalAccessException, SQLException{
	  InputStream is = fs.open(path);
                  String url = "jdbc:mysql://127.0.0.1:3306/GeoSpatialDB";
        String user = "cloudera";
        String password = "cloudera";

        // Load the Connector/J driver
        Class.forName("com.mysql.jdbc.Driver").newInstance();
        // Establish connection to MySQL
        Connection conn = DriverManager.getConnection(url, user, password);
	  String readLine;
	  BufferedReader br = new BufferedReader(new InputStreamReader(is));
	   
	  while (((readLine = br.readLine()) != null)) {
               saveItemToDB(readLine,conn);
	  System.out.println(readLine);
	  
	  
  }
  }
    
}
  

