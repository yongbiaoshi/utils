package com.tsingda.utils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.springframework.util.Assert;

public class RandomLoadBalance implements LoadBalance {

    private Map<String, List<String>> serverCacheMap = new HashMap<String, List<String>>();

    public RandomLoadBalance() {
    }

    @Override
    public String select(String path, CuratorFramework zkClient) throws Exception {
        List<String> children = serverCacheMap.get(path);
        if(children == null || children.size() == 0){
            children = updateCache(path, zkClient);
            //监听节点变化
            PathChildrenCacheListener listener = new PathChildrenCacheListener() {
                @Override
                public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
                    System.out.println("监听事件触发");
                    updateCache(path, zkClient);
                }
            };
            ZkClientUtils.addChildrenWatcher(zkClient, path, listener );
        }
        String server = null;
        if (children == null || children.size() == 0) {

        } else if (children.size() == 1) {
            server = children.get(0);
        } else {
            server = children.get(new Random().nextInt(children.size()));
        }
        return server;
    }

    @Override
    public List<String> updateCache(String path, CuratorFramework zkClient) throws Exception {
        Assert.notNull(zkClient, "Zookeeper Client can not be null!");
        List<String> children = zkClient.getChildren().forPath(path);
        serverCacheMap.put(path, children);
        return children;
    }
}
