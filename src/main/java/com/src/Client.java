package com.src;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * @ClassName Client
 * @Author Kurisu
 * @Description
 * @Date 2021-3-13 19:34
 * @Version 1.0
 **/
public class Client {
    public static void main(String[] args) throws IOException {
        RPCProtocol client = RPC.getProxy(
                RPCProtocol.class,
                RPCProtocol.versionID,
                new InetSocketAddress("localhost", 8888),
                new Configuration()
        );

        System.out.println(">>>客户端创建");
        System.out.println("请求创建/input目录");
        client.mkdirs("/input");
    }
}
