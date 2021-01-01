package com.lxfly.mr.sort2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author lxfly
 * @create 2020-12-31 11:46
 */
public class FlowBeanSortMapper extends Mapper <LongWritable, Text, FlowBean, NullWritable> {

    private FlowBean k =  new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] words = value.toString().split("\t");

        long sumUpFlow = Long.parseLong(words[1]);
        long sumDownFlow = Long.parseLong(words[2]);
        long sumFlow = Long.parseLong(words[3]);

        k.setPhoneNum(words[0]);
        k.setUpFlow(sumUpFlow);
        k.setDownFlow(sumDownFlow);
        k.setSumFlow(sumFlow);

        context.write(k,NullWritable.get());
    }
}

















