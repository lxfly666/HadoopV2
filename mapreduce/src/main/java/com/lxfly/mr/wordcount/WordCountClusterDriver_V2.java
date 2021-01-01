package com.lxfly.mr.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URISyntaxException;

/**
 * @author lxfly
 * @create 2020-12-27 0:03
 */
public class WordCountClusterDriver_V2  {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {

        System.setProperty("HADOOP_USER_NAME","lxfly");
        Configuration conf = new Configuration();

        conf.set("mapreduce.framework.name","yarn");
        conf.set("yarn.resourcemanager.hostname","hadoop103");
        conf.set("fs.defaultFS","hdfs://hadoop102:9000");

        //意思是跨平台提交，在windows下如果没有这句代码会报错 "/bin/bash: line 0: fg: no job control"，去网上搜答案很多都说是linux和windows环境不同导致的一般都是修改YarnRunner.java，但是其实添加了这行代码就可以了。
        conf.set("mapreduce.app-submission.cross-platform", "true");
        //conf.set("mapreduce.job.jar","E:\\Code\\2020practice\\HadoopV2_code\\out\\artifacts\\mapreduce_jar");


        FileSystem fs = FileSystem.get(conf);
        System.out.println(fs);

        Job job = Job.getInstance(conf);

        job.setJobName("wordcount");
        //job.setJarByClass(WordCountClusterDriver_V2.class);
        job.setJar("E:\\Code\\2020practice\\HadoopV2_code\\mapreduce\\target\\mapreduce-1.0-SNAPSHOT.jar");


        Path inputPath = new Path("hdfs://hadoop102:9000/mrinput/wordcount/");
        Path outputPath = new Path("hdfs://hadoop102:9000/mroutput/wordcount/");


        if(fs.exists(outputPath)){
            fs.delete(outputPath,true);
            System.out.println("删除原有目录wordcount");
        }

        FileInputFormat.setInputPaths(job,inputPath);
        FileOutputFormat.setOutputPath(job,outputPath);
        
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        boolean is_success = job.waitForCompletion(true);   
        if(is_success){
            System.out.println("程序运行结束！");
        }
    }
}


