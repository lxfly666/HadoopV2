package com.lxfly.mr.keyvalue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author lxfly
 * @create 2020-12-27 0:03
 */
public class WordCountLocalDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {


        Configuration conf = new Configuration();
        conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator","***");

        FileSystem fs = FileSystem.get(conf);
        Job job = Job.getInstance(conf);

        job.setJobName("keyvalue");
        job.setJarByClass(WordCountLocalDriver.class);

        Path inputPath = new Path("E:\\Code\\2020practice\\HadoopV2_code\\mapreduce\\mrinput\\keyvalue");
        Path outputPath = new Path("E:\\Code\\2020practice\\HadoopV2_code\\mapreduce\\mroutput\\keyvalue");
        if(fs.exists(outputPath)){
            fs.delete(outputPath,true);
        }

        FileInputFormat.setInputPaths(job,inputPath);
        FileOutputFormat.setOutputPath(job,outputPath);

        job.setInputFormatClass(KeyValueTextInputFormat.class);

        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        boolean is_success = job.waitForCompletion(true);   
        if(is_success){
            System.out.println("程序运行结束！");
        }
    }
}
















