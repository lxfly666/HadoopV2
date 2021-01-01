package com.lxfly.hdfsclient;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.zookeeper.common.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * @author lxfly
 * @create 2020-12-20 23:41
 */
public class HDFSClient {

    private FileSystem fs;

    @Before
    public void before() throws IOException, InterruptedException {
        //方法一：
        //fs = FileSystem.get(URI.create("hdfs://hadoop102:9000"), new Configuration(), "lxfly");
        //方法二
        System.setProperty("HADOOP_USER_NAME", "lxfly");
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://hadoop102:9000");
        //conf.set("dfs.replication","3");
        fs = FileSystem.get(conf);
        System.out.println(fs);
        System.out.println("Before!!!!!!");

    }


    @After
    public void after() throws IOException {
        System.out.println("After!!!!!!");
        if(fs!=null){
            fs.close();
        }
    }

    @Test
    public void mkdirs() throws IOException {
        boolean b = fs.mkdirs(new Path("/mroutput/wordcount"));
        System.out.println(b);
    }

    @Test
    public void put() throws IOException, InterruptedException {
        fs.copyFromLocalFile(false, true, new Path("D:\\test3.txt"), new Path("/mrinput/wordcount/test3.txt"));
        //测试配置参数优先级
/*        Configuration conf = new Configuration();
        conf.setInt("dfs.replication",10);
        FileSystem fs = FileSystem.get(URI.create("hdfs://hadoop102:9000"), conf, "lxfly");
        fs.copyFromLocalFile(new Path("d:\\test1.txt"), new Path("/test/testy.txt"));*/
    }

    @Test
    public void get() throws IOException {
        fs.copyToLocalFile(false, new Path("/mroutput/wordcount/part-r-00000"),new Path("d:\\temp\\"));
    }

    @Test
    public void rename() throws IOException{
        boolean b = fs.rename(new Path("/test11"), new Path("/testx"));
        System.out.println(b);
    }

    @Test
    public void delete() throws IOException {
        boolean b = fs.delete(new Path("/wordcount"), true);
        if(b){
            System.out.println("删除成功");
        }
    }


    @Test
    public void isFile() throws IOException{
        FileStatus fileStatus = fs.getFileStatus(new Path("/testx"));
        if(fileStatus.isDirectory()){
            System.out.println("当前path是目录");
        }else{
            System.out.println("当前path是文件");
        }
    }


    @Test
    public void ls() throws IOException{
        FileStatus[] fileStatuses = fs.listStatus(new Path("/testx"));
        for(FileStatus fileStatus:fileStatuses){
            if(fileStatus.isFile()){
                System.out.println("---以下是一个文件的信息---");
                System.out.println(fileStatus.getPath());
                System.out.println(fileStatus.getLen());
                System.out.println(fileStatus.getBlockSize());
            }else if(fileStatus.isDirectory()){
                System.out.println("---这是一个文件夹---");
                System.out.println(fileStatus.getPath());
            }else{

            }
        }
    }

    /**
     * 只递归查询所有文件
     * @throws IOException
     */
    @Test
    public void listFiles() throws IOException{
        RemoteIterator<LocatedFileStatus> files = fs.listFiles(new Path("/"), true);
        while(files.hasNext()){
            LocatedFileStatus file = files.next();
            System.out.println("\n========================================");
            System.out.println(file.getPath());
            System.out.println("块信息：");
            BlockLocation[] blockLocations = file.getBlockLocations();
            System.out.println(blockLocations.length);
            for (BlockLocation blockLocation : blockLocations) {
                String[] hosts = blockLocation.getHosts();
                for (String host : hosts) {
                    System.out.print(host+" ");
                }
            }
        }
    }

    //手动实现文件上传下载
    @Test
    public void upload() throws IOException{
        FileInputStream fileInputStream = new FileInputStream("d:\\aaa.txt");
        FSDataOutputStream fsDataOutputStream = fs.create(new Path("/testx/testy/testaaa.txt"), true);
        org.apache.hadoop.io.IOUtils.copyBytes(fileInputStream,fsDataOutputStream,fs.getConf(),true);
    }
    
    //手动实现文件上传下载
    @Test
    public void download() throws IOException{
        FSDataInputStream open = fs.open(new Path("/testx/testy/testaaa.txt"));
        FileOutputStream fileOutputStream = new FileOutputStream("d:\\bbb.txt");
        org.apache.hadoop.io.IOUtils.copyBytes(open,fileOutputStream,fs.getConf(),true);
    }

    @Test
    public void append() throws IOException{

        FSDataOutputStream out = fs.append(new Path("/mrinput/wordcount/hello.txt"), 1024);
        FileInputStream in = new FileInputStream("d:\\test1.txt");
        IOUtils.copyBytes(in, out, 1024, true);
    }


    @Test
    public void readFileSeek1() throws IOException {
        FSDataInputStream fis = fs.open(new Path("/test/hadoop-2.7.2.tar.gz"));
        FileOutputStream fos = new FileOutputStream(new File("d:\\myhadoop.tar.gz.part1"));
        byte[] buf = new byte[1024];
        for (int i = 0; i <1024*128 ; i++) {
            fis.read(buf);
            fos.write(buf);
        }
        IOUtils.closeStream(fis);
        IOUtils.closeStream(fos);
    }

    @Test
    public void readFileSeek2() throws IOException {
        FSDataInputStream fis = fs.open(new Path("/test/hadoop-2.7.2.tar.gz"));
        fis.seek(1024*1024*128);
        FileOutputStream fos = new FileOutputStream(new File("d:\\myhadoop.tar.gz.part2"));
        org.apache.hadoop.io.IOUtils.copyBytes(fis,fos,new Configuration());
        IOUtils.closeStream(fis);
        IOUtils.closeStream(fos);
    }

}
