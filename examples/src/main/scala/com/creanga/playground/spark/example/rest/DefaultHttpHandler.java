package com.creanga.playground.spark.example.rest;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import java.io.IOException;
import java.io.OutputStream;

public class DefaultHttpHandler implements HttpHandler {
    protected void response(HttpExchange httpExchange, int status, String text) throws IOException {
        httpExchange.sendResponseHeaders(status, text.length());
        OutputStream os = httpExchange.getResponseBody();
        os.write(text.getBytes());
        os.close();
    }

    public void handle(HttpExchange httpExchange) throws IOException {
        if ("GET".equals(httpExchange.getRequestMethod())) {
            handleGetRequest(httpExchange);
        } else if ("POST".equals(httpExchange.getRequestMethod())) {
            handlePostRequest(httpExchange);
        } else if ("PUT".equals(httpExchange.getRequestMethod())) {
            handlePutRequest(httpExchange);
        } else if ("DELETE".equals(httpExchange.getRequestMethod())) {
            handleDeleteRequest(httpExchange);
        } else if ("PATCH".equals(httpExchange.getRequestMethod())) {
            handlePatchRequest(httpExchange);
        } else if ("CONNECT".equals(httpExchange.getRequestMethod())) {
            handleConnectRequest(httpExchange);
        } else if ("OPTIONS".equals(httpExchange.getRequestMethod())) {
            handleOptionsRequest(httpExchange);
        } else if ("TRACE".equals(httpExchange.getRequestMethod())) {
            handleTraceRequest(httpExchange);
        } else if ("HEAD".equals(httpExchange.getRequestMethod())) {
            handleHeadRequest(httpExchange);
        }
    }

    protected void methodNotHandled(HttpExchange httpExchange) throws IOException {
        response(httpExchange, 405, "method not handled");
    }

    protected void handlePostRequest(HttpExchange httpExchange) throws IOException {
        methodNotHandled(httpExchange);
    }

    protected void handleGetRequest(HttpExchange httpExchange) throws IOException {
        methodNotHandled(httpExchange);
    }

    protected void handleHeadRequest(HttpExchange httpExchange) throws IOException {
        methodNotHandled(httpExchange);
    }

    protected void handlePutRequest(HttpExchange httpExchange) throws IOException {
        methodNotHandled(httpExchange);
    }

    protected void handleDeleteRequest(HttpExchange httpExchange) throws IOException {
        methodNotHandled(httpExchange);
    }

    protected void handleConnectRequest(HttpExchange httpExchange) throws IOException {
        methodNotHandled(httpExchange);
    }

    protected void handleTraceRequest(HttpExchange httpExchange) throws IOException {
        methodNotHandled(httpExchange);
    }

    protected void handlePatchRequest(HttpExchange httpExchange) throws IOException {
        methodNotHandled(httpExchange);
    }

    protected void handleOptionsRequest(HttpExchange httpExchange) throws IOException {
        methodNotHandled(httpExchange);
    }
}
