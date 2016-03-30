package com.tsingda.utils;

import java.util.List;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCache.StartMode;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZkClientUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(ZkClientUtils.class);
    // 重试策略
    protected static RetryPolicy RETRY_POLICY = new ExponentialBackoffRetry(2000, 5);
    // Zk namespace 所有zk操作都基于这个ZNode
    protected static String NAMESPACE = "rpc";
    private static int SESSION_TIMEOUT = 15000;

    public static CuratorFramework newClient(String zkConnectString) {
        CuratorFramework client = CuratorFrameworkFactory.builder().connectString(zkConnectString)
                .sessionTimeoutMs(SESSION_TIMEOUT).retryPolicy(RETRY_POLICY).namespace(NAMESPACE).build();
        return client;
    }

    public static void closeClient(CuratorFramework client) {
        if (client != null) {
            client.close();
        }
    }

    public static Stat checkExists(CuratorFramework client, String path) throws Exception {
        return client.checkExists().forPath(path);
    }

    public static String createNode(CuratorFramework client, String path, CreateMode mode, byte[] data,
            BackgroundCallback callback) throws Exception {
        String result = null;
        if (callback == null) {
            result = client.create().creatingParentsIfNeeded().withMode(mode).withACL(Ids.OPEN_ACL_UNSAFE).forPath(path, data);
        }else{
            result = client.create().creatingParentsIfNeeded().withMode(mode).withACL(Ids.OPEN_ACL_UNSAFE).inBackground(callback).forPath(path, data);
        }
        LOGGER.info("创建Node Path：{}， Data：{}", path, data);
        return result;
    }

    public static void deleteNode(CuratorFramework client, String path) throws Exception {
        BackgroundCallback callback = new BackgroundCallback() {
            public void processResult(CuratorFramework client, CuratorEvent event) throws Exception {
                LOGGER.info("删除结果：{}", event.getResultCode());
                LOGGER.info("删除Node事件回调：eventPath={}, eventData={}, eventType={}, eventCode={}", event.getPath(),
                        event.getData(), event.getType(), event.getResultCode());
            }
        };
        client.delete().guaranteed().deletingChildrenIfNeeded().inBackground(callback).forPath(path);
    }

    public static void deleteNode(CuratorFramework client, String path, BackgroundCallback callback) throws Exception {
        client.delete().guaranteed().deletingChildrenIfNeeded().inBackground(callback).forPath(path);
    }

    /**
     * 读取ZNode数据
     *
     * @param path Node path
     * @param stat Node stat，可以传null
     * @return
     * @throws Exception
     */
    public static byte[] readNode(CuratorFramework client, String path, Stat stat) throws Exception {
        if (stat == null) {
            stat = new Stat();
        }
        byte[] data = client.getData().storingStatIn(stat).forPath(path);
        return data;
    }

    public static void updateNode(CuratorFramework client, String path, byte[] data, int version) throws Exception {
        client.setData().withVersion(version).forPath(path, data);
    }

    public static void updateNode(CuratorFramework client, String path, byte[] data, int version,
            BackgroundCallback callback) throws Exception {
        client.setData().withVersion(version).inBackground(callback).forPath(path, data);
    }

    public static List<String> getChildren(CuratorFramework client, String path) throws Exception {
        Stat stat = new Stat();
        List<String> children = client.getChildren().storingStatIn(stat).forPath(path);
        return children;
    }

    public static void addWatcher(CuratorFramework client, String path, NodeCacheListener listener) throws Exception {
        @SuppressWarnings("resource")
        final NodeCache nodeCache = new NodeCache(client, path);
        nodeCache.start(true);
        nodeCache.getListenable().addListener(listener);
    }

    public static PathChildrenCache addChildrenWatcher(CuratorFramework client, String path, PathChildrenCacheListener listener)
            throws Exception {
        @SuppressWarnings("resource")
        final PathChildrenCache cache = new PathChildrenCache(client, path, true);
        cache.start(StartMode.NORMAL);
        cache.getListenable().addListener(listener);
        return cache;
    }

}
