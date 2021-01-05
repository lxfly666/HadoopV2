package com.lxfly.zookeeper.example;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author lxfly
 * @create 2021-01-05 14:20
 */
public class Routor {

    private String connectionString = "hadoop102:2181,hadoop103:2181,hadoop104:2181";
    private int sessionTimeout = 50000;
    private ZooKeeper zooKeeper;
    private String parentPath = "/Servers";

    public void init() throws IOException {
        zooKeeper = new ZooKeeper(connectionString, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                System.out.println(event);
            }
        });
    }

    private List<String> getNewData() throws KeeperException, InterruptedException {
        List<String> servers = new ArrayList<>();
        List<String> children = zooKeeper.getChildren(parentPath, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                System.out.println(event.getPath()+"=======》数据发生了变化！");
                try {
                    List<String> newServer = getNewData();
                    System.out.println("当前可用的服务有："+newServer);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
        for (String child : children) {
            byte[] data = zooKeeper.getData(parentPath + "/" + child, false, null);
            servers.add(new String(data));
        }
        return servers;
    }

    private void doOtherBusiness() {
        System.out.println("运行其他的逻辑......");
        try {
            Thread.sleep(Long.MAX_VALUE);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws Exception {
        Routor routor = new Routor();
        routor.init();
        List<String> servers = routor.getNewData();
        System.out.println("当前可用的服务有："+servers);
        routor.doOtherBusiness();
    }

}
