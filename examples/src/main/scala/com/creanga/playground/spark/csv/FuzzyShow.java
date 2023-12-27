package com.creanga.playground.spark.csv;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class FuzzyShow {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .master("local[16]")
                .appName("Reconciliation")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();
        Dataset<Row> ds = spark.read().parquet("/tmp/joined2");
        ds.printSchema();
        ds.createOrReplaceGlobalTempView("ds");
        System.out.println(ds.count());
        ds.show(100, false);
        //ds.select("select datasetA.domain,datasetA.name,datasetB.domain,datasetB.name  from ds ").show(100, false);
    }
}
