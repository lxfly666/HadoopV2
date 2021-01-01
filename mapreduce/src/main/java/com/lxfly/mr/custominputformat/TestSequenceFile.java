package com.lxfly.mr.custominputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * @author lxfly
 * @create 2020-12-31 9:27
 */
public class TestSequenceFile {
    public static void main(String[] args) throws IOException {
        Path path = new Path("E:\\Code\\2020practice\\HadoopV2_code\\mapreduce\\mroutput\\custom\\part-r-00000");
        Configuration conf = new Configuration();
        FileSystem fileSystem = FileSystem.get(conf);
        SequenceFile.Reader reader = new SequenceFile.Reader(fileSystem, path, conf);

        Text key = new Text();
        BytesWritable val = new BytesWritable();

        while(reader.next(key,val)){
            System.out.println(key.toString());
            System.out.println(new String(val.getBytes()));
        }
    }
}
