/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.anmpout.geomapreducejob;

import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;




/**
 *
 * @author cloudera
 */
public class GeoInputFormat extends FileInputFormat<GeoKey,GeoValue> {

    @Override
    public RecordReader<GeoKey, GeoValue> createRecordReader(InputSplit is, TaskAttemptContext tac) throws IOException, InterruptedException {
        return new GeoRecordReader();
    }

    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
    CompressionCodec codec =
    new CompressionCodecFactory(context.
    getConfiguration()).getCodec(filename);
    return codec == null;

    }
    
    


    
    
}
