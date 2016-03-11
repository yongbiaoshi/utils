package com.tsingda.utils;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import org.apache.thrift.TServiceClient;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

public class TClientUtils {

    public static <T extends TServiceClient> T newTClient(String ip, int port, int timeout, Class<T> clientClazz) {
        return newTClient(newTTransport(ip, port, timeout), clientClazz);
    }

    public static <T extends TServiceClient> T newTClient(TTransport transport, Class<T> clientClazz) {
        TProtocol protocol = new TCompactProtocol(transport);
        T client = null;
        try {
            Constructor<T> constructor = clientClazz.getDeclaredConstructor(TProtocol.class);
            client = constructor.newInstance(protocol);
            transport.open();
        } catch (InstantiationException | IllegalAccessException | TTransportException | NoSuchMethodException
                | SecurityException | IllegalArgumentException | InvocationTargetException e) {
            e.printStackTrace();
        }

        return client;
    }

    public static TTransport newTTransport(String ip, int port, int timeout) {
        TTransport transport = new TFramedTransport(new TSocket(ip, port, timeout));
        return transport;
    }
}
