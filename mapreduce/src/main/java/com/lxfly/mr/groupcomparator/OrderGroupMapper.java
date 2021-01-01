package com.lxfly.mr.groupcomparator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author lxfly
 * @create 2021-01-01 19:48
 */
public class OrderGroupMapper extends Mapper<LongWritable, Text,OrderBean, NullWritable>{

    private OrderBean k = new OrderBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] words = value.toString().split("\t");

        long orderId = Long.parseLong(words[0]);
        String productId = words[1];
        double amount = Double.parseDouble(words[2]);

        k.setOrderId(orderId);
        k.setProductId(productId);
        k.setAmount(amount);

        context.write(k, NullWritable.get());
    }
}





























