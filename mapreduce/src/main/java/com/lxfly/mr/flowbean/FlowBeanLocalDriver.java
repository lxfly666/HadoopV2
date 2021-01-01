package com.lxfly.mr.flowbean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author lxfly
 * @create 2020-12-27 0:03
 */
public class FlowBeanLocalDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {


        Configuration conf = new Configuration();

        FileSystem fs = FileSystem.get(conf);
        Job job = Job.getInstance(conf);

        job.setJobName("flowbean");
        job.setJarByClass(FlowBeanLocalDriver.class);

        Path inputPath = new Path("E:\\Code\\2020practice\\HadoopV2_code\\mapreduce\\mrinput\\flowbean");
        Path outputPath = new Path("E:\\Code\\2020practice\\HadoopV2_code\\mapreduce\\mroutput\\flowbean");
        if(fs.exists(outputPath)){
            fs.delete(outputPath,true);
        }

        FileInputFormat.setInputPaths(job,inputPath);
        FileOutputFormat.setOutputPath(job,outputPath);
        
        job.setMapperClass(FlowBeanMapper.class);
        job.setReducerClass(FlowBeanReducer.class);
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        job.setPartitionerClass(PhoneNumPartitioner.class);
        job.setNumReduceTasks(5);

        boolean is_success = job.waitForCompletion(true);   
        if(is_success){
            System.out.println("程序运行结束！");
        }
    }
}
















