package com.lxfly.mr.customoutputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * @author lxfly
 * @create 2021-01-01 20:43
 */
public class MyRecordWriter extends RecordWriter<Text, NullWritable> {

    private FileSystem fileSystem;
    private Path atguiguPath = new Path("E:\\Code\\2020practice\\HadoopV2_code\\mapreduce\\mroutput\\outputformat\\atguigu.log");
    private Path otherPath = new Path("E:\\Code\\2020practice\\HadoopV2_code\\mapreduce\\mroutput\\outputformat\\other.log");
    FSDataOutputStream atguiguOs;
    FSDataOutputStream otherOs;
    TaskAttemptContext context;

    public MyRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException {
        context = taskAttemptContext;
        Configuration configuration = taskAttemptContext.getConfiguration();
        fileSystem = FileSystem.get(configuration);
        atguiguOs = fileSystem.create(atguiguPath, true);
        otherOs = fileSystem.create(otherPath, true);
    }

    @Override
    public void write(Text text, NullWritable nullWritable) throws IOException, InterruptedException {
        if(text.toString().contains("atguigu")){
            atguiguOs.write(text.toString().getBytes());
            context.getCounter("MyCounter","atguiguCounter").increment(1);
        }else{
            otherOs.write(text.toString().getBytes());
            context.getCounter("MyCounter","otherCounter").increment(1);
        }
    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {

        if(atguiguOs!=null){
            IOUtils.closeStream(atguiguOs);
        }
        if(otherOs!=null){
            IOUtils.closeStream(otherOs);
        }
        if(fileSystem!=null){
            fileSystem.close();
        }
    }
}

















