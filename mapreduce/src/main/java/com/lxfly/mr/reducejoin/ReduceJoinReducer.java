package com.lxfly.mr.reducejoin;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author lxfly
 * @create 2021-01-01 22:43
 */
public class ReduceJoinReducer extends Reducer<Text,JoinBean,JoinBean, NullWritable> {

    private List<JoinBean> orderData = new ArrayList<>();
    private Map<String, String>  pdData = new HashMap<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
    }

    @Override
    protected void reduce(Text key, Iterable<JoinBean> values, Context context) throws IOException, InterruptedException {

        if(key.toString().contains("order")){
            for (JoinBean value : values) {
                JoinBean joinBean = new JoinBean();
                try {
                    BeanUtils.copyProperties(joinBean,value);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                orderData.add(joinBean);
            }
        }else{
            for (JoinBean value : values) {
                pdData.put(value.getPid(), value.getPname());
            }
        }

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (JoinBean joinBean : orderData) {
            joinBean.setPname(pdData.get(joinBean.getPid()));
            context.write(joinBean,NullWritable.get());
        }
    }
}






















