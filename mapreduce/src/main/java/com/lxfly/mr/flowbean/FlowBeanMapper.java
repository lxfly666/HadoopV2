package com.lxfly.mr.flowbean;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author lxfly
 * @create 2020-12-30 20:25
 */
public class FlowBeanMapper extends Mapper <LongWritable, Text, Text, FlowBean>{

    private Text k = new Text();
    private FlowBean v = new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] words = value.toString().split("\t");
        long downFlow = Long.parseLong(words[words.length - 2]);
        long upFlow = Long.parseLong(words[words.length - 3]);

        k.set(words[1]);

        v.setUpFlow(upFlow);
        v.setDownFlow(downFlow);

        context.write(k,v);
    }

}














