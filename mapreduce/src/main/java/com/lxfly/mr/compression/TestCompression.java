package com.lxfly.mr.compression;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Test;

import java.io.*;

/**
 * @author lxfly
 * @create 2021-01-02 14:59
 */
public class TestCompression {

    @Test
    public void testCompression() throws ClassNotFoundException, IOException {
        String codeClassName = "org.apache.hadoop.io.compress.GzipCodec";
        Class<?> aClass = Class.forName(codeClassName);
        Configuration conf = new Configuration();
        CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(aClass, conf);

        CompressionOutputStream os = codec.createOutputStream(new FileOutputStream("d:/file"+codec.getDefaultExtension()));

        FSDataInputStream is = FileSystem.get(conf).open(new Path("d:/hello.txt"));

        IOUtils.copyBytes(is,os,4096,true);
    }

    @Test
    public void testDecompression() throws IOException {
        Configuration conf = new Configuration();
        CompressionCodec codec = (new CompressionCodecFactory(conf)).getCodec(new Path("d:/file.gz"));

        CompressionInputStream is = codec.createInputStream(new FileInputStream(new File("d:/file.gz")));
        FSDataOutputStream os = FileSystem.get(conf).create(new Path("d:/file.txt"), true);

        IOUtils.copyBytes(is,os,4096,true);

    }
}


















