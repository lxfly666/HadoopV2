package com.lxfly.zookeeper.test;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

/**
 * @author lxfly
 * @create 2021-01-05 11:13
 */
public class TestZookeeper {

    private String connectionString = "hadoop102:2181,hadoop103:2181,hadoop104:2181";
    private int sessionTimeout = 50000;
    private ZooKeeper zooKeeper;

    @Before
    public void init() throws IOException {
        zooKeeper = new ZooKeeper(connectionString, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent event) {

            }
        });
    }

    @After
    public void close() throws InterruptedException {
        if(zooKeeper!=null){
            zooKeeper.close();
        }
    }

    @Test
    public void addZnode() throws KeeperException, InterruptedException {
        zooKeeper.create("/idea","hello".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    @Test
    public void modifyData() throws KeeperException, InterruptedException {
        zooKeeper.setData("/idea","newHello".getBytes(),-1);
    }

    @Test
    public void delZnode() throws Exception {
        zooKeeper.delete("/idea",-1);
    }

    @Test
    public void ls() throws Exception {
        Stat stat = new Stat();
        List<String> children = zooKeeper.getChildren("/x", false, stat);
        for (String child : children) {
            System.out.println(child);
        }
        System.out.println(stat);
    }

    @Test
    public void getData() throws Exception {
        Stat stat = new Stat();
        byte[] data = zooKeeper.getData("/x", false, stat);
        System.out.println(new String(data));
    }

    public String getData2() throws Exception {
        byte[] data = zooKeeper.getData("/x", new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                System.out.println(event.getPath()+"=======》数据发生了变化！");
                try {
                    String newData = getData2();
                    System.out.println("新的数据是："+newData);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        },null);
        return new String(data);
    }

    @Test
    public void testAlwaysWatch() throws Exception {
        String data2 = getData2();
        System.out.println("节点的值是"+data2);
        Thread.sleep(100000);
    }
}



















