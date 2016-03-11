package com.tsingda.utils;

import java.util.List;

import org.apache.curator.framework.CuratorFramework;

public interface LoadBalance {
    String select(String path, CuratorFramework zkClient) throws Exception;
    List<String> updateCache(String path, CuratorFramework zkClient) throws Exception;
}
