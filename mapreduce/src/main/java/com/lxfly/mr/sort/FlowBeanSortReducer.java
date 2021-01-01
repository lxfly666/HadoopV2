package com.lxfly.mr.sort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author lxfly
 * @create 2020-12-31 11:46
 */
public class FlowBeanSortReducer extends Reducer<LongWritable, FlowBean, NullWritable, FlowBean> {

    @Override
    protected void reduce(LongWritable key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        for (FlowBean value : values) {
            context.write(NullWritable.get(),value);
        }
    }

}

















