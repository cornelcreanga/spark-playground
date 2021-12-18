package com.creanga.playground.spark.example.metrics;

import ch.cern.sparkmeasure.StageMetrics;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.List;

public class SparkMeasureMultiThreadedJobsJava{

    public static void main(String[] args) throws Exception{
        SparkSession sparkSession = SparkSession.builder()
                .master("local[4]")
                .appName("Spark streaming")
                .getOrCreate();
        SparkContext sc = sparkSession.sparkContext();


        StageMetrics stageMetrics = new StageMetrics(sparkSession);
        List<Thread> threads = new ArrayList<>();
        for (int i = 1; i <= 5; i++) {
            final int counter = i;
            Thread thread = new Thread(() -> {
                sc.setLocalProperty("spark.jobGroup.id",""+counter);
                Dataset<Row> dataset = sparkSession.read().load("/home/cornel/data" + counter + ".parquet");
                stageMetrics.begin();
                long countResult = dataset.filter(dataset.col("_verizon.emsLessDns").equalTo("Y")).count();
                System.out.println(countResult);
                stageMetrics.end();
            });
            threads.add(thread);
            thread.start();
        }

        for (int i = 0; i < 5; i++)  {
            threads.get(i).join();
        }


        Dataset<Row> metrics = stageMetrics.createStageMetricsDF("temp");
        metrics.show(100);

    }
}

