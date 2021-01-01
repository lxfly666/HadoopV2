package com.lxfly.mr.sort2;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author lxfly
 * @create 2020-12-31 11:37
 */
public class FlowBean implements WritableComparable<FlowBean> {

    private long upFlow;
    private long downFlow;
    private long sumFlow;
    private String phoneNum;

    public long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    public long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(long downFlow) {
        this.downFlow = downFlow;
    }

    public long getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow(long sumFlow) {
        this.sumFlow = sumFlow;
    }

    public String getPhoneNum() {
        return phoneNum;
    }

    public void setPhoneNum(String phoneNum) {
        this.phoneNum = phoneNum;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(upFlow);
        dataOutput.writeLong(downFlow);
        dataOutput.writeLong(sumFlow);
        dataOutput.writeUTF(phoneNum);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        upFlow = dataInput.readLong();
        downFlow = dataInput.readLong();
        sumFlow = dataInput.readLong();
        phoneNum = dataInput.readUTF();
    }

    @Override
    public String toString() {
        return phoneNum + "\t" + upFlow + "\t" + downFlow + "\t" + sumFlow;
    }

    @Override
    public int compareTo(FlowBean o) {
        int result = 0;
        result = this.sumFlow > o.getSumFlow() ? -1 : (this.sumFlow == o.getSumFlow() ? 0 : 1);
        if(result==0){
            result = this.upFlow > o.getUpFlow() ? 1 : (this.upFlow == o.getUpFlow() ? 0 : -1);
        }
        return result;
    }
}
