package com.creanga.playground.spark.example.rest;

import com.sun.net.httpserver.HttpExchange;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.net.URL;

public class ClassLocationHandler extends AbstractHttpHandler {

    @Override
    public void handle(HttpExchange httpExchange) throws IOException {
        if ("GET".equals(httpExchange.getRequestMethod())) {
            handleGetRequest(httpExchange);
        }
    }

    private void handleGetRequest(HttpExchange httpExchange) throws IOException {
        String uri = httpExchange.getRequestURI().toString();
        String lookupResourceName = StringUtils.substringAfter(uri, "/classlocation/");

        if (lookupResourceName == null || lookupResourceName.length() == 0) {
            response(httpExchange, 400, "missing resource name");
            return;
        }
        if (!lookupResourceName.contains("/")) {
            lookupResourceName = lookupResourceName.replaceAll("[.]", "/");
            lookupResourceName = "/" + lookupResourceName + ".class";
        }

        URL url = this.getClass().getResource(lookupResourceName);
        if (url == null) {
            response(httpExchange, 412, "unable to locate resource" + lookupResourceName);
            return;
        }
        response(httpExchange, 200, url.toExternalForm());

    }
}
