package com.creanga.playground.spark.example.compression.parquet;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class CompressionCodecs {

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder()
                .master("local[4]")
                .appName("Parquet test")
                .config("parquet.compression.codec.zstd.bufferPool.enabled","true")
                .config("parquet.compression.codec.zstd.level","3")
                .config("parquet.block.size",512 * 1024 * 1024)
                .config("parquet.page.size",4 * 1024 * 1024)
                .getOrCreate();
        JavaSparkContext sc = new JavaSparkContext(sparkSession.sparkContext());
        long t1, t2;
        Dataset<Row> parquetFileDF = sparkSession.read().
                parquet("/Users/ccreanga/Downloads/fhvhv_tripdata_2022-01.parquet").coalesce(1);
        //parquetFileDF = parquetFileDF.sortWithinPartitions("hvfhs_license_num","dispatching_base_num");
        parquetFileDF.persist();
        parquetFileDF.printSchema();
        parquetFileDF.show(5, false);
        parquetFileDF.createOrReplaceTempView("data");
        Dataset<Row> namesDF = sparkSession.sql("SELECT  count (distinct hvfhs_license_num),count (distinct dispatching_base_num),count (distinct PULocationID)  FROM data");
        namesDF.show();
//        long t1, t2;
//        t1 = System.currentTimeMillis();
//        parquetFileDF.write().mode(SaveMode.Overwrite).option("compression", "none").parquet("/tmp/parquet/no_compression");
//        t2 = System.currentTimeMillis();
//        System.out.println(t2-t1);
//        t1 = System.currentTimeMillis();
//        parquetFileDF.write().mode(SaveMode.Overwrite).option("compression", "snappy").parquet("/tmp/parquet/snappy");
//        t2 = System.currentTimeMillis();
//        System.out.println(t2-t1);
        System.out.println("Count " + parquetFileDF.count());
        t1 = System.currentTimeMillis();
        parquetFileDF.write().mode(SaveMode.Overwrite).option("compression", "zstd").parquet("/tmp/parquet/zstd");
        t2 = System.currentTimeMillis();
        System.out.println(t2-t1);


    }

}
