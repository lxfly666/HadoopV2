package com.lxfly.mr.topN;

import org.apache.hadoop.io.WritableComparator;

/**
 * @author lxfly
 * @create 2020-12-31 12:26
 */
public class MyLongWriteableComparator extends WritableComparator {
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
        long thisValue = readLong(b1, s1);
        long thatValue = readLong(b2, s2);
        return thisValue < thatValue ? 1 : (thisValue == thatValue ? 0 : -1);
    }
}
