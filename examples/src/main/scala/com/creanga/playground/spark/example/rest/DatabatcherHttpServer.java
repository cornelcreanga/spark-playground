package com.creanga.playground.spark.example.rest;

import com.sun.net.httpserver.HttpContext;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

public class DatabatcherHttpServer {

    private HttpServer server;
    private int port;

    public DatabatcherHttpServer(int port) {
        this.port = port;
    }

    public void start() throws IOException {
        ThreadPoolExecutor threadPoolExecutor = (ThreadPoolExecutor) Executors.newFixedThreadPool(4);

        server = HttpServer.create(new InetSocketAddress(port), 0);
        server.setExecutor(threadPoolExecutor);
        server.createContext("/customapi/loggers", new LoggersHeaderHandler());
        server.createContext("/customapi/classlocation", new ClassLocationHandler());
        server.start();
    }

    public void stop(){
        server.stop(5);
    }

}
