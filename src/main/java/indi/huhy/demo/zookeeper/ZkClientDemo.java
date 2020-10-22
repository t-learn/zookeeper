package indi.huhy.demo.zookeeper;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;
import org.apache.zookeeper.CreateMode;

import java.util.List;

public class ZkClientDemo {

    public static void main(String[] args) {
        // 创建一个zk客户端
        ZkClient client = new ZkClient("localhost:2181");
        client.setZkSerializer(new StringZkSerializer());

        client.subscribeChildChanges("/node", new IZkChildListener() {
            @Override
            public void handleChildChange(String s, List<String> list) throws Exception {

            }
        });

        client.subscribeDataChanges("/node", new IZkDataListener() {
            @Override
            public void handleDataChange(String s, Object o) throws Exception {

            }

            @Override
            public void handleDataDeleted(String s) throws Exception {

            }
        });

        client.create("/node", "value", CreateMode.PERSISTENT);
    }
}

