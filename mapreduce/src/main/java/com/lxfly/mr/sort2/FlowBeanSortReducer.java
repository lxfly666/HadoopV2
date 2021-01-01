package com.lxfly.mr.sort2;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author lxfly
 * @create 2020-12-31 11:46
 */
public class FlowBeanSortReducer extends Reducer<FlowBean, NullWritable, FlowBean, NullWritable> {

    @Override
    protected void reduce(FlowBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        for (NullWritable value : values) {
            context.write(key,NullWritable.get());
        }
    }
}

















