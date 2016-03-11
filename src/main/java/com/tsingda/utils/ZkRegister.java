package com.tsingda.utils;

import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.data.Stat;
import org.springframework.util.Assert;

public class ZkRegister {
    //例：192.168.2.156:2180,192.168.2.156:2181,192.168.2.156:2182
    private String zkConnectString = null;
    
    public ZkRegister(){
        
    }
    
    public ZkRegister(String connextionStr){
        this.zkConnectString = connextionStr;
    }
    
    
    public void register(String product, String service, String version, String connectionStr) throws Exception {
        Assert.hasText(connectionStr, "Zookeeper连接不能为空");
        CuratorFramework client = ZkClientUtils.newClient(zkConnectString);
        client.start();
        String path = String.format("/%s/%s/%s/%s", product, service, version, connectionStr);
        Stat stat = ZkClientUtils.checkExists(client, path);
        if(stat != null){
            ZkClientUtils.deleteNode(client, path);
        }
        ZkClientUtils.createNode(client, path, connectionStr.getBytes());
    }

    public String getZkConnectString() {
        return zkConnectString;
    }

    public void setZkConnectString(String zkConnectString) {
        this.zkConnectString = zkConnectString;
    }
}
