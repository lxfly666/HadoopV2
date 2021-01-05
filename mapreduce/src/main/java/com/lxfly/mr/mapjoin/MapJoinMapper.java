package com.lxfly.mr.mapjoin;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * @author lxfly
 * @create 2021-01-02 13:01
 */
public class MapJoinMapper extends Mapper <LongWritable, Text, JoinBean, NullWritable>{

    private Map<String,String> pdData = new HashMap<>();
    private JoinBean k = new JoinBean();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        URI[] cacheFiles = context.getCacheFiles();
        for (URI cacheFile : cacheFiles) {
            FSDataInputStream is = FileSystem.get(context.getConfiguration()).open(new Path(cacheFile));
            BufferedReader reader = new BufferedReader(new InputStreamReader(is,"utf-8"));
            String line = null;
            while(StringUtils.isNotEmpty(line = reader.readLine())){
                String[] words = line.split("\t");
                pdData.put(words[0],words[1]);
            }
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] words = value.toString().split("\t");
        long orderId = Long.parseLong(words[0]);
        String pId = words[1];
        int amount = Integer.parseInt(words[2]);

        k.setOrderId(orderId);
        k.setPname(pdData.get(pId));
        k.setAmount(amount);

        context.write(k,NullWritable.get());
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }
}
