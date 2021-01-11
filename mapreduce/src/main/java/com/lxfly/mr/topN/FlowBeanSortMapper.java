package com.lxfly.mr.topN;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author lxfly
 * @create 2020-12-31 11:46
 */
public class FlowBeanSortMapper extends Mapper <LongWritable, Text, LongWritable, FlowBean> {

    private LongWritable k = new LongWritable();
    private FlowBean v =  new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] words = value.toString().split("\t");

        long sumUpFlow = Long.parseLong(words[1]);
        long sumDownFlow = Long.parseLong(words[2]);
        long sumFlow = Long.parseLong(words[3]);

        k.set(sumFlow);
        v.setPhoneNum(words[0]);
        v.setUpFlow(sumUpFlow);
        v.setDownFlow(sumDownFlow);
        v.setSumFlow(sumFlow);

        context.write(k,v);
    }
}

















