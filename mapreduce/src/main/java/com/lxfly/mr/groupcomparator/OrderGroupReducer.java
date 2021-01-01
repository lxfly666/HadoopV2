package com.lxfly.mr.groupcomparator;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author lxfly
 * @create 2021-01-01 19:56
 */
public class OrderGroupReducer extends Reducer <OrderBean, NullWritable, OrderBean, NullWritable> {

    @Override
    protected void reduce(OrderBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        double maxAmount = key.getAmount();
        for (NullWritable value : values) {
            if(key.getAmount() != maxAmount){
                break;
            }
            context.write(key, NullWritable.get());
        }
    }
}























