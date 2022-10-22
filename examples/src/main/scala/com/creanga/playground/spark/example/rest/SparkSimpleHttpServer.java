package com.creanga.playground.spark.example.rest;

import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

public class SparkSimpleHttpServer {

    private HttpServer server;
    private int port;

    public SparkSimpleHttpServer(int port) {
        this.port = port;
    }

    public void start() throws RuntimeException {
        ThreadPoolExecutor threadPoolExecutor = (ThreadPoolExecutor) Executors.newFixedThreadPool(4);

        try {
            server = HttpServer.create(new InetSocketAddress(port), 0);
            server.setExecutor(threadPoolExecutor);
            server.createContext("/customapi/loggers", new LoggersHandler());
            server.createContext("/customapi/classlocation", new ClassLocationHandler());
            server.start();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void stop() {
        server.stop(5);
    }

}
