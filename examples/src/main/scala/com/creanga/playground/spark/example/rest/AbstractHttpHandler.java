package com.creanga.playground.spark.example.rest;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import java.io.IOException;
import java.io.OutputStream;

public abstract class AbstractHttpHandler implements HttpHandler {
    protected void response(HttpExchange httpExchange, int status, String text) throws IOException {
        httpExchange.sendResponseHeaders(status,text.length());
        OutputStream os = httpExchange.getResponseBody();
        os.write(text.getBytes());
        os.close();
    }
}
