package com.lxfly.mr.findfriend;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author lxfly
 * @create 2021-01-11 20:30
 */
public class FindFriendReducer2 extends Reducer<Text, Text,Text,Text> {

    private Text v = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        StringBuffer sb = new StringBuffer();

        for (Text value : values) {
            sb.append(value.toString()+",");
        }

        v.set(sb.toString());
        context.write(key,v);

    }
}










