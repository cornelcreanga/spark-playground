package com.creanga.playground.spark.example.rest;

import com.sun.net.httpserver.HttpExchange;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.net.URL;

public class ClassLocationHandler extends DefaultHttpHandler {

    protected void handleGetRequest(HttpExchange httpExchange) throws IOException {
        String uri = httpExchange.getRequestURI().toString();
        String resource = StringUtils.substringAfter(uri, "/classlocation/");

        if (resource == null || resource.length() == 0) {
            response(httpExchange, 400, "missing resource name");
            return;
        }
        if (!resource.contains("/")) {
            resource = resource.replaceAll("[.]", "/");
            resource = "/" + resource + ".class";
        }

        URL url = this.getClass().getResource(resource);
        if (url == null) {
            response(httpExchange, 412, "unable to locate resource" + resource);
            return;
        }
        response(httpExchange, 200, url.toExternalForm());

    }

    protected void handleOptionsRequest(HttpExchange httpExchange) throws IOException {
        httpExchange.getRequestHeaders().add("Allow", "GET,POST,OPTIONS");
        httpExchange.getRequestHeaders().add("Access-Control-Allow-Methods", "GET,POST,OPTIONS");
        httpExchange.getRequestHeaders().add("Access-Control-Allow-Headers", "Content-Type");
        response(httpExchange, 200, "ok");
    }
}
