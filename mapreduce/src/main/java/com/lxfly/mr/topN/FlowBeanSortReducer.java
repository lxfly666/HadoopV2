package com.lxfly.mr.topN;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author lxfly
 * @create 2020-12-31 11:46
 */
public class FlowBeanSortReducer extends Reducer<LongWritable, FlowBean, NullWritable, FlowBean> {

    private int sum = 0;

    @Override
    protected void reduce(LongWritable key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {

        //第一种，top10，只能输出10个
/*        for (FlowBean value : values) {
            if(sum>=10){
                break;
            }
            context.write(NullWritable.get(),value);
            sum++;
        }*/

        if(sum<10){
            for (FlowBean value : values) {
                context.write(NullWritable.get(),value);
                sum++;
            }
        }
    }

}

















