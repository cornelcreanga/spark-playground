package com.creanga.playground.spark.example.rest;

import com.sun.net.httpserver.HttpExchange;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;

public class LoggersHandler extends DefaultHttpHandler {

    protected void handlePostRequest(HttpExchange httpExchange) throws IOException {

        String uri = httpExchange.getRequestURI().toString();
        String logger = StringUtils.substringAfter(uri,"/loggers/");
        if (StringUtils.isBlank(logger)){
            response(httpExchange, 400, "missing logger name");
        }else{
            SparkSession sparkSession = SparkSession.active();
            if (sparkSession == null) {
                response(httpExchange, 500, "internal error, this should never happen unless the Spark context was not started.");
            }else{
                String level = IOUtils.toString(httpExchange.getRequestBody(), StandardCharsets.UTF_8.name());
                Level logLevel = Level.toLevel(level, Level.INFO);
                boolean rootLogger = "root".equalsIgnoreCase(logger);

                SparkContext sc = sparkSession.sparkContext();
                JavaSparkContext jsc = new JavaSparkContext(sc);

                if (rootLogger){
                    LogManager.getRootLogger().setLevel(logLevel);
                    jsc.parallelize(new ArrayList<>()).foreachPartition(objectIterator -> {
                        LogManager.getRootLogger().setLevel(logLevel);
                    });
                }else{
                    Logger.getLogger(logger).setLevel(logLevel);
                    jsc.parallelize(new ArrayList<>()).foreachPartition(objectIterator -> {
                        LogManager.getLogger(logger).setLevel(logLevel);
                    });

                }
            }

        }

    }

    protected void handleGetRequest(HttpExchange httpExchange) throws IOException {
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
