package com.lxfly.mr.flowbean;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author lxfly
 * @create 2020-12-30 20:57
 */
public class FlowBeanReducer extends Reducer<Text, FlowBean, Text, FlowBean> {

    private FlowBean v = new FlowBean();

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        long sumUpFlow = 0;
        long sumDownFlow = 0;

        for (FlowBean value : values) {
            sumUpFlow += value.getUpFlow();
            sumDownFlow += value.getDownFlow();
        }

        v.setUpFlow(sumUpFlow);
        v.setDownFlow(sumDownFlow);
        v.setSumFlow(sumUpFlow+sumDownFlow);

        context.write(key, v);
    }


}
