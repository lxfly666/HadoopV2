package com.lxfly.mr.custominputformat;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @author lxfly
 * @create 2020-12-31 0:17
 */
public class MyRecordReader extends RecordReader<Text, BytesWritable> {

    private Text key;
    private BytesWritable value;

    private FileSplit fileSplit;
    private FileSystem fileSystem;

    private Path filePath;
    private FSDataInputStream inputStream;

    private boolean flag = true;

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        key = new Text();
        value = new BytesWritable();
        fileSplit = (FileSplit) inputSplit;

        filePath = fileSplit.getPath();
        fileSystem = FileSystem.get(taskAttemptContext.getConfiguration());
        inputStream = fileSystem.open(filePath);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if(flag){
            key.set(filePath.getName());
            byte[] bytes = new byte[(int)fileSplit.getLength()];
            IOUtils.readFully(inputStream, bytes,0, bytes.length);
            value.set(bytes,0, bytes.length);
            flag = false;
            return true;
        }
        return false;
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public void close() throws IOException {
        fileSystem.close();
        inputStream.close();
    }
}
