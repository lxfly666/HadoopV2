package com.lxfly.mr.custominputformat;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author lxfly
 * @create 2020-12-31 9:16
 */
public class SequenceFileReducer extends Reducer <Text, BytesWritable, Text, BytesWritable>{
}
