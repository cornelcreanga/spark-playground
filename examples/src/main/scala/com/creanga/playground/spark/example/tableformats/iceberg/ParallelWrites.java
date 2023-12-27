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

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.*;

import static org.apache.spark.sql.types.DataTypes.*;

public class ParallelWrites {

    public static void setEnv(String key, String value) {
        try {
            Map<String, String> env = System.getenv();
            Class<?> cl = env.getClass();
            Field field = cl.getDeclaredField("m");
            field.setAccessible(true);
            Map<String, String> writableEnv = (Map<String, String>) field.get(env);
            writableEnv.put(key, value);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to set environment variable", e);
        }
    }

    public static void main(String[] args) {

        //AWS_ACCESS_KEY_ID (or AWS_ACCESS_KEY) and AWS_SECRET_KEY
        setEnv("AWS_ACCESS_KEY_ID","Q3AM3UQ867SPQQA43P2F") ;
        setEnv("AWS_SECRET_KEY","zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG");
        SparkSession sparkSession = SparkSession.builder()
                .master("local[8]")
                .appName("ParallelWrites")
                .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                .config("spark.sql.defaultCatalog", "test-write")
                .config("spark.sql.catalog.test-write", "org.apache.iceberg.spark.SparkCatalog")
                .config("spark.sql.catalog.test-write.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
                .config("spark.sql.catalog.test-write.warehouse", "s3a://ccreanga/iceberg/")
                .config("spark.sql.catalog.test-write.s3.endpoint", "https://play.min.io:50000")
                .config("spark.sql.catalog.test-write.s3.path-style-access", "true")
                .config("spark.sql.catalog.test-write.s3.access-key-id", "Q3AM3UQ867SPQQA43P2F")
                .config("spark.sql.catalog.test-write.s3.secret-access-key", "zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG")
                .config("spark.sql.catalog.test-write.type", "hadoop")
                .config("spark.sql.catalogImplementation", "in-memory")
                .config("spark.executor.heartbeatInterval", "300000")
                .config("spark.network.timeout", "400000")
                //.config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
//                .config("spark.hadoop.fs.s3a.access.key", "Q3AM3UQ867SPQQA43P2F")
//                .config("spark.hadoop.fs.s3a.secret.key", "zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG")
//                .config("spark.hadoop.fs.s3a.endpoint", "play.min.io:50000")
//                .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true")
//                .config("spark.hadoop.fs.s3a.path.style.access", "true")
//                .config("spark.hadoop.fs.s3a.attempts.maximum", "1")
//                .config("spark.hadoop.fs.s3a.connection.establish.timeout", "5000")
//                .config("spark.hadoop.fs.s3a.connection.timeout", "10000")
                .getOrCreate();

        StructType schema = new StructType(new StructField[]{
                new StructField("VendorID", LongType, true, Metadata.empty()),
                new StructField("tpep_pickup_datetime", StringType, true, Metadata.empty()),
                new StructField("tpep_dropoff_datetime", StringType, true, Metadata.empty()),
                new StructField("passenger_count", DoubleType, true, Metadata.empty()),
                new StructField("trip_distance", DoubleType, true, Metadata.empty()),
                new StructField("RatecodeID", DoubleType, true, Metadata.empty()),
                new StructField("store_and_fwd_flag", StringType, true, Metadata.empty()),
                new StructField("PULocationID", LongType, true, Metadata.empty()),
                new StructField("DOLocationID", LongType, true, Metadata.empty()),
                new StructField("payment_type", LongType, true, Metadata.empty()),
                new StructField("fare_amount", DoubleType, true, Metadata.empty()),
                new StructField("extra", DoubleType, true, Metadata.empty()),
                new StructField("mta_tax", DoubleType, true, Metadata.empty()),
                new StructField("tip_amount", DoubleType, true, Metadata.empty()),
                new StructField("tolls_amount", DoubleType, true, Metadata.empty()),
                new StructField("improvement_surcharge", DoubleType, true, Metadata.empty()),
                new StructField("total_amount", DoubleType, true, Metadata.empty())
        });

        Dataset<Row> dataset = sparkSession.read().option("header", "true").schema(schema).csv("s3://ccreanga/taxi/taxi.csv");
        dataset.cache();
        System.out.println(dataset.count());



//# Create Iceberg table "nyc.taxis_large" from RDD
//        df.write.mode("overwrite").saveAsTable("nyc.taxis_large")
//
//        SparkContext sc = new SparkContext(conf);
//        SparkSession sparkSession = SparkSession.builder().getOrCreate();
//
//        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(sc);
//        int partitions = 40;
//        int values = 100_000_000;
//
//        List<UUID> drivers = new ArrayList<>();
//        for (int i = 0; i < 10_000; i++) {
//            drivers.add(UUID.randomUUID());
//        }
//
//        TripRddProvider rddProvider = new TripRddProvider(jsc, partitions, values, new HashMap<>(), drivers);
//        JavaRDD<Trip> tripRdd = rddProvider.buildRdd();
//        //tripRdd.persist(StorageLevel.MEMORY_ONLY());
//        Dataset<Trip> dataset = sparkSession
//                .createDataset(tripRdd.rdd(), Encoders.bean(Trip.class));
//        //dataset = dataset.withColumn("day", dataset.col("orderTimestamp"));
//        dataset.
//                write().
//                option("compression","zstd").
//                partitionBy(new String[]{"orderYear","orderMonth","orderDay"}).
//                mode("overwrite").
//                parquet("/Users/cornel/parquet");
//
//        Dataset<Row> dataset2 = sparkSession.read().parquet("/Users/cornel/parquet");
//        dataset2.show(100);
        //System.out.println(dataset.schema());
        //dataset.write().mode("overwrite").saveAsTable("trips");



    }




}

