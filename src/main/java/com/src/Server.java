package com.src;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;

/**
 * @ClassName Server
 * @Author Kurisu
 * @Description
 * @Date 2021-3-13 19:30
 * @Version 1.0
 **/
public class Server implements RPCProtocol {
    public static void main(String[] args) throws IOException {
        RPC.Server server = new RPC.Builder(new Configuration())
                .setBindAddress("localhost")
                .setPort(8888)
                .setProtocol(RPCProtocol.class)
                .setInstance(new Server())
                .build();
        server.start();
        System.out.println("server启动>>>");
    }
    @Override
    public void mkdirs(String path) {
        System.out.println("在server创建"+path+"目录");
    }
}
