package com.creanga.playground.spark.example.tableformats.iceberg;

import com.creanga.playground.spark.example.partitioner.SyntheticRddProvider;
import com.creanga.playground.spark.example.tableformats.Trip;
import com.creanga.playground.spark.example.tableformats.TripRddProvider;
import org.apache.commons.lang3.RandomUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

public class ParallelWrites {



    public static void main(String[] args) {

        SparkConf conf = new SparkConf()
                .setMaster("local[8]")
                .setAppName("ParallelWrites")
                .set("spark.driver.memoryOverheadFactor", "0.05")
                .set("spark.memory.fraction", "0.8");

//        SparkSession sparkSession = SparkSession.builder()
//                .master("local[8]")
//                .appName("ParallelWrites")
//                .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
//                .config("spark.sql.defaultCatalog", "testwrite")
//                .config("spark.sql.catalog.testwrite", "org.apache.iceberg.spark.SparkCatalog")
//                .config("spark.sql.catalog.testwrite.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
//                .config("spark.sql.catalog.testwrite.warehouse", "s3a://ccreanga/iceberg/")
//                .config("spark.sql.catalog.testwrite.s3.endpoint", "https://play.min.io:50000")
//                .config("spark.sql.catalogImplementation", "in-memory")
//                .config("spark.sql.catalog.testwrite.type", "hadoop")
//                .config("spark.executor.heartbeatInterval", "300000")
//                .config("spark.network.timeout", "400000")
//                .config("spark.hadoop.fs.s3a.access.key", "Q3AM3UQ867SPQQA43P2F")
//                .config("spark.hadoop.fs.s3a.secret.key", "zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG")
//                .config("spark.hadoop.fs.s3a.endpoint", "play.min.io:50000")
//                .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true")
//                .config("spark.hadoop.fs.s3a.path.style.access", "true")
//                .config("spark.hadoop.fs.s3a.attempts.maximum", "1")
//                .config("spark.hadoop.fs.s3a.connection.establish.timeout", "5000")
//                .config("spark.hadoop.fs.s3a.connection.timeout", "10000")
//                .getOrCreate();

        StructType schema = new StructType(new StructField[]{
                new StructField("id", DataTypes.StringType, false, Metadata.empty()),
                new StructField("name", DataTypes.StringType, false, Metadata.empty()),
                new StructField("surname", DataTypes.StringType, false, Metadata.empty()),
                new StructField("points", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("birthday", DataTypes.DateType, false, Metadata.empty())});

        SparkContext sc = new SparkContext(conf);
        SparkSession sparkSession = SparkSession.builder().getOrCreate();

        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(sc);
        int partitions = 40;
        int values = 100_000_000;

        List<UUID> drivers = new ArrayList<>();
        for (int i = 0; i < 10_000; i++) {
            drivers.add(UUID.randomUUID());
        }

        TripRddProvider rddProvider = new TripRddProvider(jsc, partitions, values, new HashMap<>(), drivers);
        JavaRDD<Trip> tripRdd = rddProvider.buildRdd();
        //tripRdd.persist(StorageLevel.MEMORY_ONLY());
        Dataset<Trip> dataset = sparkSession
                .createDataset(tripRdd.rdd(), Encoders.bean(Trip.class));
        //dataset = dataset.withColumn("day", dataset.col("orderTimestamp"));
        dataset.
                write().
                option("compression","zstd").
                partitionBy(new String[]{"orderYear","orderMonth","orderDay"}).
                mode("overwrite").
                parquet("/Users/cornel/parquet");

        Dataset<Row> dataset2 = sparkSession.read().parquet("/Users/cornel/parquet");
        dataset2.show(100);
        //System.out.println(dataset.schema());
        //dataset.write().mode("overwrite").saveAsTable("trips");



    }




}

