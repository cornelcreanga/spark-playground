package com.creanga.playground.spark.example.customize;

import org.apache.spark.sql.SparkSession;

public class ExampleRestApi {


    public static void main(String[] args) throws InterruptedException {
        SparkSession sparkSession = SparkSession.builder()
                .master("local[3]")
                .appName("ExampleRestApi")
                .config("spark.master.rest.enabled", "true")
                .getOrCreate();

        Thread.sleep(600000);

    }

}
