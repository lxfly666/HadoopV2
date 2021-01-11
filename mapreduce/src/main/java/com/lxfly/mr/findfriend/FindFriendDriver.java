package com.lxfly.mr.findfriend;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @author lxfly
 * @create 2021-01-11 20:57
 */
public class FindFriendDriver {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        Path inputPath = new Path("E:\\Code\\2020practice\\HadoopV2_code\\mapreduce\\mrinput\\friend");
        Path outputPath = new Path("E:\\Code\\2020practice\\HadoopV2_code\\mapreduce\\mroutput\\friend");
        Path finalOutputPath = new Path("E:\\Code\\2020practice\\HadoopV2_code\\mapreduce\\mroutput\\friend2");

        if(fs.exists(outputPath)){
            fs.delete(outputPath,true);
        }
        if(fs.exists(finalOutputPath)){
            fs.delete(finalOutputPath,true);
        }

        conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator",":");
        Job job1 = Job.getInstance(conf);
        job1.setJobName("friend1");
        job1.setJarByClass(FindFriendDriver.class);
        FileInputFormat.setInputPaths(job1,inputPath);
        FileOutputFormat.setOutputPath(job1,outputPath);
        job1.setMapperClass(FindFriendMapper.class);
        job1.setReducerClass(FindFriendReducer.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setInputFormatClass(KeyValueTextInputFormat.class);
        ControlledJob controlledJob1 = new ControlledJob(job1.getConfiguration());

        Configuration conf2 = new Configuration();
        conf2.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator","\t");
        Job job2 = Job.getInstance(conf2);
        job2.setJobName("friend2");
        job2.setJarByClass(FindFriendDriver.class);
        FileInputFormat.setInputPaths(job2,outputPath);
        FileOutputFormat.setOutputPath(job2,finalOutputPath);
        job2.setMapperClass(FindFriendMapper2.class);
        job2.setReducerClass(FindFriendReducer2.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setInputFormatClass(KeyValueTextInputFormat.class);
        ControlledJob controlledJob2 = new ControlledJob(job2.getConfiguration());
        controlledJob2.addDependingJob(controlledJob1);

        JobControl jobControl = new JobControl("findfriend");

        jobControl.addJob(controlledJob1);
        jobControl.addJob(controlledJob2);
        jobControl.run();

    }

}
