package com.creanga.playground.spark.example.logging;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.SparkSession;

public class LoggingTest {

    private static final Logger LOG = LogManager.getLogger(LoggingTest.class);

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder()
                .master("local[2]")
                .appName("Spark example")
                .getOrCreate();

//-Dlog4j.configuration= log4j2.configurationFile
        String level = LogManager.getLogger("root").getLevel().toString();
        System.out.println("root log level is " + level);
        LOG.error("error");
        LOG.warn("warn");
        LOG.info("info");
        LOG.debug("debug");
        LOG.trace("trace");
        try {
            throw new RuntimeException("RuntimeException");
        } catch (Exception e) {
            LOG.error("exception", e);
        }


    }
}
