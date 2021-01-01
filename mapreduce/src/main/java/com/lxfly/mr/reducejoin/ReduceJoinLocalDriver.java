package com.lxfly.mr.reducejoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author lxfly
 * @create 2021-01-01 23:01
 */
public class ReduceJoinLocalDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();

        FileSystem fs = FileSystem.get(conf);
        Job job = Job.getInstance(conf);

        job.setJobName("reducejoin");
        job.setJarByClass(ReduceJoinLocalDriver.class);

        Path inputPath = new Path("E:\\Code\\2020practice\\HadoopV2_code\\mapreduce\\mrinput\\reducejoin");
        Path outputPath = new Path("E:\\Code\\2020practice\\HadoopV2_code\\mapreduce\\mroutput\\reducejoin");
        if(fs.exists(outputPath)){
            fs.delete(outputPath,true);
        }

        FileInputFormat.setInputPaths(job,inputPath);
        FileOutputFormat.setOutputPath(job,outputPath);

        job.setMapperClass(ReduceJoinMapper.class);
        job.setReducerClass(ReduceJoinReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(JoinBean.class);
        job.setOutputKeyClass(JoinBean.class);
        job.setOutputValueClass(NullWritable.class);

        boolean is_success = job.waitForCompletion(true);
        if(is_success){
            System.out.println("程序运行结束！");
        }
    }
}
