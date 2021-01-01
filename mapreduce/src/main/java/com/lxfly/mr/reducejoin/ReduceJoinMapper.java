package com.lxfly.mr.reducejoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @author lxfly
 * @create 2021-01-01 22:19
 */
public class ReduceJoinMapper extends Mapper <LongWritable, Text,Text,JoinBean>{

    private Text k = new Text();
    private JoinBean v = new JoinBean();
    private String filename;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        FileSplit split = (FileSplit) context.getInputSplit();
        filename = split.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        k.set(filename);
        String[] words = value.toString().split("\t");
        if(filename.contains("order")){
            long orderId = Long.parseLong(words[0]);
            String pId = words[1];
            int amount = Integer.parseInt(words[2]);

            v.setOrderId(orderId);
            v.setPid(pId);
            v.setAmount(amount);
            v.setPname("nodata");
        }else{
            v.setPid(words[0]);
            v.setPname(words[1]);
        }
        context.write(k, v);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }
}























