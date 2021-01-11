package com.lxfly.mr.index;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author lxfly
 * @create 2021-01-10 22:37
 */
public class IndexMapper2 extends Mapper <Text, Text, Text, Text>{

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        context.write(key, value);
    }
}
