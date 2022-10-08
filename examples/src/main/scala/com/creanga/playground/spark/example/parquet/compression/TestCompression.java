package com.creanga.playground.spark.example.parquet.compression;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;

public class TestCompression {

    public static void main(String[] args) {

        SparkSession sparkSession = SparkSession.builder()
                .master("local[8]")
                .appName("Spark streaming")
                .getOrCreate();

        SparkContext sc = sparkSession.sparkContext();

        long t1 = System.currentTimeMillis();
        Dataset<Row> parquetFileDF = sparkSession.read().parquet("/home/cornel/parquetdata/init");
        parquetFileDF.persist(StorageLevel.MEMORY_ONLY_SER());
        long t2 = System.currentTimeMillis();
        System.out.println("read:" + (t2 - t1));


        t1 = System.currentTimeMillis();
        parquetFileDF.
                write().
                mode(SaveMode.Overwrite).
                option("compression", "none").
                parquet("/home/cornel/parquetdata/none");
        t2 = System.currentTimeMillis();
        System.out.println("write none" + (t2 - t1));

        t1 = System.currentTimeMillis();
        parquetFileDF.
                write().
                mode(SaveMode.Overwrite).
                option("compression", "zstd").
                parquet("/home/cornel/parquetdata/zstd");
        t2 = System.currentTimeMillis();
        System.out.println("write zstd" + (t2 - t1));

        t1 = System.currentTimeMillis();
        parquetFileDF.
                write().
                mode(SaveMode.Overwrite).
                option("compression", "gzip").
                parquet("/home/cornel/parquetdata/gzip");
        t2 = System.currentTimeMillis();
        System.out.println("write gzip" + (t2 - t1));

        t1 = System.currentTimeMillis();
        parquetFileDF.
                write().
                mode(SaveMode.Overwrite).
                option("compression", "snappy").
                parquet("/home/cornel/parquetdata/snappy");
        t2 = System.currentTimeMillis();
        System.out.println("write snappy" + (t2 - t1));

        t1 = System.currentTimeMillis();
        parquetFileDF.
                write().
                mode(SaveMode.Overwrite).
                option("compression", "lz4").
                parquet("/home/cornel/parquetdata/lz4");
        t2 = System.currentTimeMillis();
        System.out.println("write lz4" + (t2 - t1));

    }
}
