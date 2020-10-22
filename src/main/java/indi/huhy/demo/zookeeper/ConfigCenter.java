package indi.huhy.demo.zookeeper;

import org.I0Itec.zkclient.ZkClient;

/**
 * 配置中心示例
 */
public class ConfigCenter {

    // 1、 写入配置
    public void putConfig(String configPath, String value) {
        ZkClient client = new ZkClient("localhost:2181");
        client.setZkSerializer(new StringZkSerializer());
        if (client.exists(configPath)) {
            client.writeData(configPath, value);
        } else {
            client.createPersistent(configPath, value);
        }
        client.close();
    }

    // 2、 读取配置
    public String getConfig(String configPath) {
        ZkClient client = new ZkClient("localhost:2181");
        client.setZkSerializer(new StringZkSerializer());
        return client.readData(configPath);
    }
}
