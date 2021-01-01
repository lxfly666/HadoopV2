package com.lxfly.mr.groupcomparator;


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
 * @create 2021-01-01 20:18
 */
public class OrderGroupLocalDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();

        FileSystem fs = FileSystem.get(conf);
        Job job = Job.getInstance(conf);

        job.setJobName("ordergroup");
        job.setJarByClass(OrderGroupLocalDriver.class);

        Path inputPath = new Path("E:\\Code\\2020practice\\HadoopV2_code\\mapreduce\\mrinput\\groupcomparator");
        Path outputPath = new Path("E:\\Code\\2020practice\\HadoopV2_code\\mapreduce\\mroutput\\groupcomparator");
        if(fs.exists(outputPath)){
            fs.delete(outputPath,true);
        }

        FileInputFormat.setInputPaths(job,inputPath);
        FileOutputFormat.setOutputPath(job,outputPath);

        job.setMapperClass(OrderGroupMapper.class);
        job.setReducerClass(OrderGroupReducer.class);

        job.setMapOutputKeyClass(OrderBean.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(OrderBean.class);
        job.setOutputValueClass(NullWritable.class);

        job.setGroupingComparatorClass(MyGroupComparator.class);

        boolean is_success = job.waitForCompletion(true);
        if(is_success){
            System.out.println("程序运行结束！");
        }
    }
}
