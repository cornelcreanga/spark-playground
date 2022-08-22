package com.creanga.playground.spark.example.rest;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.LogManager;

import java.io.IOException;
import java.io.OutputStream;

public class LoggersHeaderHandler extends AbstractHttpHandler {
    @Override
    public void handle(HttpExchange httpExchange) throws IOException {
        if("GET".equals(httpExchange.getRequestMethod())) {
            handleGetRequest(httpExchange);
        }else if("POST".equals(httpExchange.getRequestMethod())) {
            handlePostRequest(httpExchange);
        }
    }

    private void handlePostRequest(HttpExchange httpExchange) {
        String uri = httpExchange.getRequestURI().toString();
    }

    private void handleGetRequest(HttpExchange httpExchange) throws IOException {
        String uri = httpExchange.getRequestURI().toString();
        String logger = StringUtils.substringAfter(uri,"/loggers/");
        if (StringUtils.isBlank(logger)){
            response(httpExchange, 400, "missing logger name");
        }else{
            String level = LogManager.getLogger(logger).getLevel().toString();
            response(httpExchange, 200, level);
        }
    }


}
