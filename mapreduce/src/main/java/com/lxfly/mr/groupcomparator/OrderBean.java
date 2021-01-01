package com.lxfly.mr.groupcomparator;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author lxfly
 * @create 2021-01-01 19:27
 */
public class OrderBean implements WritableComparable <OrderBean>{

    private long orderId;
    private String productId;
    private double amount;

    @Override
    public String toString() {
        return orderId+"\t"+productId+"\t"+amount;
    }

    public long getOrderId() {
        return orderId;
    }

    public void setOrderId(long orderId) {
        this.orderId = orderId;
    }

    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    public double getAmount() {
        return amount;
    }

    public void setAmount(double amount) {
        this.amount = amount;
    }

    @Override
    public int compareTo(OrderBean o) {

        int result = this.getOrderId() > o.getOrderId() ? 1 : (this.getOrderId() == o.getOrderId() ? 0 : -1);
        if(result == 0){
            result = this.getAmount() > o.getAmount() ? -1 : (this.getAmount() == o.getAmount() ? 0 : 1);
        }
        return result;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(orderId);
        dataOutput.writeUTF(productId);
        dataOutput.writeDouble(amount);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        orderId = dataInput.readLong();
        productId = dataInput.readUTF();
        amount = dataInput.readDouble();
    }
}





























