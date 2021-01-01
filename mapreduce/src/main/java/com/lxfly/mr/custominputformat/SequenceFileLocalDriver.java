package com.lxfly.mr.custominputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

/**
 * @author lxfly
 * @create 2020-12-31 9:18
 */
public class SequenceFileLocalDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {


        Configuration conf = new Configuration();

        FileSystem fs = FileSystem.get(conf);
        Job job = Job.getInstance(conf);

        job.setJobName("sequencefile");
        job.setJarByClass(SequenceFileLocalDriver.class);

        Path inputPath = new Path("E:\\Code\\2020practice\\HadoopV2_code\\mapreduce\\mrinput\\custom");
        Path outputPath = new Path("E:\\Code\\2020practice\\HadoopV2_code\\mapreduce\\mroutput\\custom");
        if(fs.exists(outputPath)){
            fs.delete(outputPath,true);
        }

        FileInputFormat.setInputPaths(job,inputPath);
        FileOutputFormat.setOutputPath(job,outputPath);

        job.setInputFormatClass(MyInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setMapperClass(SequenceFileMapper.class);
        job.setReducerClass(SequenceFileReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(BytesWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);

        boolean is_success = job.waitForCompletion(true);
        if(is_success){
            System.out.println("程序运行结束！");
        }
    }

}
