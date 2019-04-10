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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
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

    private static void saveItemToDBRealTime(String readLine, Connection conn) {
        try {
            Calendar cal = Calendar.getInstance();
            double speed = 0.0;
            int    count = 0;
            int pathId = 0;
            long currentTimestamp = 0L;
            List<FilterData> profileDayData = new ArrayList<>();
            // the mysql insert statement
            String query = " insert into FILTER_DATA_RT (PATH_ID,TIMESTAMP ,COUNT, SPEED,"
                    + " TIME, MAX_SPEED, MIN_SPEED, MEDIAN_SPEED, DAY, MONTH, YEAR)"
                    + " values (?, ?, ?, ?, ?,?, ?, ?, ?,?,?)";

            // create the mysql insert preparedstatement
            String[] parts = readLine.split("\t");
            if (!parts[2].equals("") && !parts[2].equals("NaN")) {
            count = Integer.parseInt(parts[2]);
            }
            if (!parts[3].equals("") && !parts[3].equals("NaN")) {
            speed = Double.parseDouble(parts[3]);
            }
            if (!parts[0].equals("") && !parts[0].equals("NaN")) {
                pathId = Integer.parseInt(parts[0]);
               
            }
            System.out.println(pathId);
            if(pathId != 0){
         
            int dayOfWeekInMonth = cal.get(Calendar.DAY_OF_WEEK_IN_MONTH);
            int dayOfWeek = cal.get(Calendar.DAY_OF_WEEK);
            int year = cal.get(Calendar.YEAR);       
            int month = cal.get(Calendar.MONTH);      
            long previousYearTimestamp = getSameDayOfYearD(year-1,month,dayOfWeek,dayOfWeekInMonth).getTime()/1000;
            long previous2YearTimestamp = getSameDayOfYearD(year-2,month,dayOfWeek,dayOfWeekInMonth).getTime()/1000;
            String queryProfileSameDay = " select * from FILTER_DATA T WHERE"
                    + " (T.TIMESTAMP = ? OR T.TIMESTAMP = ?)"
                    + " AND T.PATH_ID = ? ORDER BY T.TIMESTAMP ";
            PreparedStatement preparedStmtpProfileDay = conn.prepareStatement(queryProfileSameDay);
            preparedStmtpProfileDay.setLong(1, previousYearTimestamp);
            preparedStmtpProfileDay.setLong(2, previous2YearTimestamp);
            preparedStmtpProfileDay.setLong(3, pathId);
                
            ResultSet rs = preparedStmtpProfileDay.executeQuery();
             System.out.println(rs.first());
            while(rs.next()){
                FilterData filterData = new FilterData();
                filterData.setPathId(rs.getInt(1));
                filterData.setTimestamp(rs.getLong(2));
                filterData.setTime(rs.getInt(3));
                filterData.setCount(rs.getInt(4));
                filterData.setSpeed(rs.getDouble(6));
                filterData.setDay(rs.getInt(rs.getInt(7)));
                filterData.setMonth(rs.getInt(8));
                filterData.setYear(rs.getInt(9));
                filterData.setMedianSpeed(rs.getInt(10));
                filterData.setMaxSpeed(rs.getInt(11));
                filterData.setMinSpeed(rs.getInt(12));
                profileDayData.add(filterData);
                System.out.println(filterData.toString());
            
            }
            
            }
            PreparedStatement preparedStmt = conn.prepareStatement(query);
            //PATH_ID
            if (!parts[0].equals("") && !parts[0].equals("NaN")) {
                preparedStmt.setInt(1, Integer.parseInt(parts[0]));
            } else {
                preparedStmt.setInt(1, 0);
            }
            //TIMESTAMP
            if (!parts[1].equals("") && !parts[1].equals("NaN")) {
                preparedStmt.setLong(2, Long.parseLong(parts[1]));
            } else {
                preparedStmt.setLong(2, 0);
            }
            //COUNT
            if (!parts[2].equals("") && !parts[2].equals("NaN")) {
                preparedStmt.setInt(3, Integer.parseInt(parts[2]));
            } else {
                preparedStmt.setInt(3, 0);
            }
            //SPEED
            if (!parts[3].equals("") && !parts[3].equals("NaN")) {
                preparedStmt.setDouble(4, Double.parseDouble(parts[3]));
            } else {
                preparedStmt.setDouble(4, 0);
            }
            //TIME         
            if (!parts[4].equals("") && !parts[4].equals("NaN")) {
                preparedStmt.setInt(5, Integer.parseInt(parts[4]));
            } else {
                preparedStmt.setInt(5, 0);
            }
              //MAX         
            if (!parts[6].equals("") && !parts[6].equals("NaN")) {
                preparedStmt.setDouble(6, Double.parseDouble(parts[6]));
            } else {
                preparedStmt.setDouble(6, 0);
            }
                                    //min         
            if (!parts[7].equals("") && !parts[7].equals("NaN")) {
                preparedStmt.setDouble(7, Double.parseDouble(parts[7]));
            } else {
                preparedStmt.setDouble(7, 0);
            }
             //median         
            if (!parts[8].equals("") && !parts[8].equals("NaN")) {
                preparedStmt.setDouble(8, Double.parseDouble(parts[8]));
            } else {
                preparedStmt.setDouble(8, 0);
            }
            //day         
            if (!parts[9].equals("") && !parts[9].equals("NaN")) {
                preparedStmt.setInt(9, Integer.parseInt(parts[9]));
            } else {
                preparedStmt.setInt(9, 0);
            }
           //month         
            if (!parts[10].equals("") && !parts[10].equals("NaN")) {
                preparedStmt.setInt(10, Integer.parseInt(parts[10]));
            } else {
                preparedStmt.setInt(10, 0);
            }
             //year         
            if (!parts[11].equals("") && !parts[11].equals("NaN")) {
                preparedStmt.setInt(11, Integer.parseInt(parts[11]));
            } else {
                preparedStmt.setInt(11, 0);
            }
            
            

            // execute the preparedstatement
            preparedStmt.execute();
            //create profile for current record
            createProfileForCurrentDay(profileDayData,currentTimestamp,speed,count);
            
            
            

        } catch (Exception e) {
            System.err.println(e.getMessage());

        }    }

    private static void createProfileForCurrentDay(List<FilterData> profileDayData, long currentTimestamp, double speed, int count) {
     
        System.out.println("createProfileForCurrentDay");
        System.out.println(profileDayData.size());
        System.out.println(currentTimestamp);
        System.out.println(speed);
        System.out.println(count);
    }

    @Override
    public int run(String[] args) throws Exception {
        String outputPath = "/user/thesis/sample1/output/"
                + Long.toString(Calendar.getInstance().getTimeInMillis());

        Job job = Job.getInstance(getConf(), "GeoFilter");
        job.setJarByClass(this.getClass());
        try (Writer writer = new BufferedWriter(new OutputStreamWriter(
                new FileOutputStream("/home/cloudera/Downloads/mapperoutput.txt"), "utf-8"))) {
            writer.write(outputPath);
        }
        boolean realTime = Boolean.parseBoolean(args[1]);
        boolean saveStats = Boolean.parseBoolean(args[2]);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        job.setMapperClass(GeoFilterMapper.class);
        job.setMapOutputKeyClass(GeoKeyMap.class);
        job.setMapOutputValueClass(GeoValue.class);
        job.setInputFormatClass(GeoInputFormat.class);
        job.setReducerClass(GeoFilterReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        if (job.waitForCompletion(true) && job.isSuccessful() && saveStats) {
            Configuration hadoopConfig = new Configuration();
            hadoopConfig.set("fs.defaultFS", "hdfs://quickstart.cloudera:8020");
            hadoopConfig.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
            FileSystem fs = FileSystem.get(hadoopConfig);
            Path inputPath2 = new Path(outputPath.trim().replace("\n", "") + "/part-r-00000");
            openFileFromHDFS(fs, inputPath2,realTime);

        }

        return job.waitForCompletion(true) ? 0 : 1;

    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new GeoFilter(), args);
        System.exit(exitCode);

    }

    private static void saveItemToDB(String readLine, Connection conn) {
        try {
            // the mysql insert statement
            String query = " insert into FILTER_DATA (PATH_ID,TIMESTAMP ,COUNT, SPEED,"
                    + " TIME, MAX_SPEED, MIN_SPEED, MEDIAN_SPEED, DAY, MONTH, YEAR)"
                    + " values (?, ?, ?, ?, ?,?, ?, ?, ?,?,?)";

            // create the mysql insert preparedstatement
            String[] parts = readLine.split("\t");
            PreparedStatement preparedStmt = conn.prepareStatement(query);
            //PATH_ID
            if (!parts[0].equals("") && !parts[0].equals("NaN")) {
                preparedStmt.setInt(1, Integer.parseInt(parts[0]));
            } else {
                preparedStmt.setInt(1, 0);
            }
            //TIMESTAMP
            if (!parts[1].equals("") && !parts[1].equals("NaN")) {
                preparedStmt.setLong(2, Long.parseLong(parts[1]));
            } else {
                preparedStmt.setLong(2, 0);
            }
            //COUNT
            if (!parts[2].equals("") && !parts[2].equals("NaN")) {
                preparedStmt.setInt(3, Integer.parseInt(parts[2]));
            } else {
                preparedStmt.setInt(3, 0);
            }
            //SPEED
            if (!parts[3].equals("") && !parts[3].equals("NaN")) {
                preparedStmt.setDouble(4, Double.parseDouble(parts[3]));
            } else {
                preparedStmt.setDouble(4, 0);
            }
            //TIME         
            if (!parts[4].equals("") && !parts[4].equals("NaN")) {
                preparedStmt.setInt(5, Integer.parseInt(parts[4]));
            } else {
                preparedStmt.setInt(5, 0);
            }
                        //MAX         
            if (!parts[6].equals("") && !parts[6].equals("NaN")) {
                preparedStmt.setDouble(6, Double.parseDouble(parts[6]));
            } else {
                preparedStmt.setDouble(6, 0);
            }
                                    //min         
            if (!parts[7].equals("") && !parts[7].equals("NaN")) {
                preparedStmt.setDouble(7, Double.parseDouble(parts[7]));
            } else {
                preparedStmt.setDouble(7, 0);
            }
             //median         
            if (!parts[8].equals("") && !parts[8].equals("NaN")) {
                preparedStmt.setDouble(8, Double.parseDouble(parts[8]));
            } else {
                preparedStmt.setDouble(8, 0);
            }
                        //day         
            if (!parts[9].equals("") && !parts[9].equals("NaN")) {
                preparedStmt.setInt(9, Integer.parseInt(parts[9]));
            } else {
                preparedStmt.setInt(9, 0);
            }
                                    //month         
            if (!parts[10].equals("") && !parts[10].equals("NaN")) {
                preparedStmt.setInt(10, Integer.parseInt(parts[10]));
            } else {
                preparedStmt.setInt(10, 0);
            }
                                                //year         
            if (!parts[11].equals("") && !parts[11].equals("NaN")) {
                preparedStmt.setInt(11, Integer.parseInt(parts[11]));
            } else {
                preparedStmt.setInt(11, 0);
            }

            // execute the preparedstatement
            preparedStmt.execute();

        } catch (Exception e) {
            System.err.println(e.getMessage());

        }

    }

    public static void openFileFromHDFS(FileSystem fs, Path path,boolean realTime) throws IOException, ClassNotFoundException, InstantiationException, IllegalAccessException, SQLException {
        InputStream is = fs.open(path);
//        String url = "jdbc:mysql://127.0.0.1:3306/GeoSpatialDB";
//        String user = "cloudera";
//        String password = "cloudera";
//
//        // Load the Connector/J driver
Class.forName("com.mysql.jdbc.Driver").newInstance();
Connection conn=DriverManager.getConnection(  
"jdbc:mysql://geofilterdb.c4nkehdtywwc.us-east-2.rds.amazonaws.com:3306/geofilterdb","cloudera","cloudera"); 
      
        String readLine;
        BufferedReader br = new BufferedReader(new InputStreamReader(is));
        if(realTime){
                while (((readLine = br.readLine()) != null)) {
            saveItemToDBRealTime(readLine,conn);
            System.out.println(readLine);

        }    
            
        }else{
        while (((readLine = br.readLine()) != null)) {
            saveItemToDB(readLine,conn);
            System.out.println(readLine);

        }
        }
        conn.close();
    }
public static Date getSameDayOfYearD(int year, int month, int day, int week) {
    Calendar cal = Calendar.getInstance();
    cal.set(Calendar.DAY_OF_WEEK, day);
    cal.set(Calendar.DAY_OF_WEEK_IN_MONTH, week);
    cal.set(Calendar.MONTH, month);
    cal.set(Calendar.YEAR, year);
    cal.set(Calendar.HOUR,cal.get(Calendar.HOUR)-1);
    cal.set(Calendar.MINUTE,0);
    cal.set(Calendar.SECOND,0);
    cal.set(Calendar.MILLISECOND,0);
    System.out.println(cal.getTime());
    return cal.getTime();
}
}
