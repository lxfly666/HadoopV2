package com.lxfly.mr.findfriend;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;

/**
 * @author lxfly
 * @create 2021-01-11 20:17
 */
public class FindFriendMapper2 extends Mapper<Text, Text,Text,Text> {
    private Text k = new Text();

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {

        String[] users = key.toString().split("-");
        Arrays.sort(users);

        for (int i = 0; i <users.length-1 ; i++) {
            for (int j = i+1; j <users.length ; j++) {
                k.set(users[i]+"-"+users[j]);
                context.write(k,value);
            }
        }

    }
}
