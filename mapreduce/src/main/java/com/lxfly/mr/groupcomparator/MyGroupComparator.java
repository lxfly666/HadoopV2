package com.lxfly.mr.groupcomparator;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @author lxfly
 * @create 2021-01-01 20:07
 */
public class MyGroupComparator extends WritableComparator{

    public MyGroupComparator() {
        super(OrderBean.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {

        OrderBean o1 = (OrderBean)a;
        OrderBean o2 = (OrderBean)b;

        int result = o1.getOrderId() > o2.getOrderId() ? 1 : (o1.getOrderId() == o2.getOrderId() ? 0 : -1);

        return result;
    }
}
