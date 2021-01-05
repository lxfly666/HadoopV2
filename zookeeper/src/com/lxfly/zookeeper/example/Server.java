package com.lxfly.zookeeper.example;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;

/**
 * @author lxfly
 * @create 2021-01-05 11:55
 */
public class Server {

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

    private void check() throws KeeperException, InterruptedException {
        Stat stat = zooKeeper.exists(parentPath, false);
        if(stat==null){
            String path = zooKeeper.create(parentPath, "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            System.out.println(path+"======>创建成功");
        }
    }

    private void register(String data) throws KeeperException, InterruptedException {
        zooKeeper.create(parentPath+"/Server",data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.EPHEMERAL_SEQUENTIAL);
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
        Server server = new Server();
        server.init();
        server.check();
        server.register(args[0]);
        server.doOtherBusiness();
    }

}














