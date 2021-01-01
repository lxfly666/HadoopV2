package com.lxfly.mr.sort2;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author lxfly
 * @create 2020-12-31 10:44
 */
public class PhoneNumPartitioner extends Partitioner <FlowBean, NullWritable> {

    @Override
    public int getPartition(FlowBean key, NullWritable value, int numPartitions) {

        String phoneNumPrefix = key.getPhoneNum().substring(0, 3);
        int partition = 0;

        switch (phoneNumPrefix){

            case "136":
                partition = 1;
                break;

            case "137":
                partition = 2;
                break;

            case "138":
                partition = 3;
                break;

            case "139":
                partition = 4;
                break;

            default:
                break;
        }
        return partition;
    }

}
