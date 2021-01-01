package com.lxfly.mr.custominputformat;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author lxfly
 * @create 2020-12-31 9:15
 */
public class SequenceFileMapper extends Mapper <Text, BytesWritable, Text, BytesWritable>{
}
