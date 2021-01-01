package com.lxfly.mr.sort2;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author lxfly
 * @create 2020-12-27 0:03
 */
public class FlowBeanSortLocalDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();

        FileSystem fs = FileSystem.get(conf);
        Job job = Job.getInstance(conf);

        job.setJobName("sort2");
        job.setJarByClass(FlowBeanSortLocalDriver.class);

        Path inputPath = new Path("E:\\Code\\2020practice\\HadoopV2_code\\mapreduce\\mrinput\\sort");
        Path outputPath = new Path("E:\\Code\\2020practice\\HadoopV2_code\\mapreduce\\mroutput\\sort2");
        if(fs.exists(outputPath)){
            fs.delete(outputPath,true);
        }

        FileInputFormat.setInputPaths(job,inputPath);
        FileOutputFormat.setOutputPath(job,outputPath);
        
        job.setMapperClass(FlowBeanSortMapper.class);
        job.setReducerClass(FlowBeanSortReducer.class);
        
        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(FlowBean.class);
        job.setOutputValueClass(NullWritable.class);

        job.setNumReduceTasks(5);
        job.setPartitionerClass(PhoneNumPartitioner.class);

        boolean is_success = job.waitForCompletion(true);   
        if(is_success){
            System.out.println("程序运行结束！");
        }
    }
}
















