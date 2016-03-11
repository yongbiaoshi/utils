package com.tsingda.utils;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.thrift.TProcessor;
import org.apache.thrift.TProcessorFactory;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadedSelectorServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TServerUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(TServerUtils.class);
    public static TServer newTServer(int port, TProcessor processor) throws TTransportException{
        LOGGER.info("start server");
        TNonblockingServerSocket serverTransport = null;
        TThreadedSelectorServer server = null;
        try {
            serverTransport = new TNonblockingServerSocket(port);

            // 目前Thrift提供的最高级的模式，可并发处理客户端请求 
            TThreadedSelectorServer.Args ttssArgs = new TThreadedSelectorServer.Args(serverTransport);
            ttssArgs.processor(processor);

            // 设置协议工厂，高效率的、密集的二进制编码格式进行数据传输协议
            ttssArgs.protocolFactory(new TCompactProtocol.Factory()); //

            // 设置传输工厂，使用非阻塞方式，按块的大小进行传输，类似于Java中的NI
            ttssArgs.transportFactory(new TFramedTransport.Factory()); //

            // 设置处理器工厂，只返回一个单例实例
            ttssArgs.processorFactory(new TProcessorFactory(processor));

            // 多个线程，主要负责客户端的IO处理  1.事件注册 2.读写IO轮询 3.将数据读取或者写入到transport
            ttssArgs.selectorThreads(2);
            ttssArgs.acceptQueueSizePerThread(10);

            // 工作线程池 
            ExecutorService pool = Executors.newCachedThreadPool();
            ttssArgs.executorService(pool);

            server = new TThreadedSelectorServer(ttssArgs);
            LOGGER.info("HelloWorld TThreadedSelectorServer start ....");
            return server;
        } catch (TTransportException e) {
            LOGGER.error("服务端错误信息：{}", e.getMessage());
            throw e;
        }
    }
}
