package com.lxfly.mr.findfriend;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author lxfly
 * @create 2021-01-11 20:17
 */
public class FindFriendMapper extends Mapper<Text, Text,Text,Text> {
    private Text k = new Text();

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {

        String[] friends = value.toString().split(",");
        for (String friend : friends) {
            k.set(friend);
            context.write(k,key);
        }
    }
}
