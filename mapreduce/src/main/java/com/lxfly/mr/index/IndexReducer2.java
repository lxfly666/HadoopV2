package com.lxfly.mr.index;

import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * @author lxfly
 * @create 2021-01-10 22:40
 */
public class IndexReducer2 extends Reducer<Text,Text, Text,Text> {

    private Text v = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuffer sb = new StringBuffer();
        for (Text value : values) {
            sb.append(value.toString()+" ");
        }
        v.set(sb.toString());
        context.write(key, v);
    }
}
