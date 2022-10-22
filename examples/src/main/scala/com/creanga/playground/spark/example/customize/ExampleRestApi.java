package com.creanga.playground.spark.example.customize;

import com.creanga.playground.spark.example.rest.SparkSimpleHttpServer;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;

public class ExampleRestApi {


    public static void main(String[] args) throws InterruptedException, IOException {
        SparkSession sparkSession = SparkSession.builder()
                .master("local[3]")
                .appName("ExampleRestApi")
                .config("spark.master.rest.enabled", "true")
                .getOrCreate();

        SparkSimpleHttpServer sparkSimpleHttpServer = new SparkSimpleHttpServer(4041);
        sparkSimpleHttpServer.start();
        Thread.sleep(600000);

    }

}
