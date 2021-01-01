package com.lxfly.mr.customoutputformat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author lxfly
 * @create 2021-01-01 21:05
 */
public class CustomInputFormatMapper extends Mapper <LongWritable, Text, Text, NullWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        value.set(value.toString()+"\r\n");
        context.write(value,NullWritable.get());
    }
}

