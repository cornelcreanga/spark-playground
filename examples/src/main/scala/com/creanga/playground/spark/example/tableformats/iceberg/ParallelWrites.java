package com.creanga.playground.spark.example.tableformats.iceberg;

import com.creanga.playground.spark.example.tableformats.Trip;
import org.apache.commons.lang3.RandomUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class ParallelWrites {

    public static final double initLatitude = 44.481230877283856d;
    public static final double initLongitude = 26.1745387998047d;

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder()
                .master("local[8]")
                .appName("ParallelWrites")
                .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                .config("spark.sql.defaultCatalog", "testwrite")
                .config("spark.sql.catalog.testwrite", "org.apache.iceberg.spark.SparkCatalog")
                .config("spark.sql.catalog.testwrite.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
                .config("spark.sql.catalog.testwrite.warehouse", "s3a://ccreanga/iceberg/")
                .config("spark.sql.catalog.testwrite.s3.endpoint", "https://play.min.io:50000")
                .config("spark.sql.catalogImplementation", "in-memory")
                .config("spark.sql.catalog.testwrite.type", "hadoop")
                .config("spark.executor.heartbeatInterval", "300000")
                .config("spark.network.timeout", "400000")
                .config("spark.hadoop.fs.s3a.access.key", "Q3AM3UQ867SPQQA43P2F")
                .config("spark.hadoop.fs.s3a.secret.key", "zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG")
                .config("spark.hadoop.fs.s3a.endpoint", "play.min.io:50000")
                .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true")
                .config("spark.hadoop.fs.s3a.path.style.access", "true")
                .config("spark.hadoop.fs.s3a.attempts.maximum", "1")
                .config("spark.hadoop.fs.s3a.connection.establish.timeout", "5000")
                .config("spark.hadoop.fs.s3a.connection.timeout", "10000")
                .getOrCreate();

        StructType schema = new StructType(new StructField[]{
                new StructField("id", DataTypes.StringType, false, Metadata.empty()),
                new StructField("name", DataTypes.StringType, false, Metadata.empty()),
                new StructField("surname", DataTypes.StringType, false, Metadata.empty()),
                new StructField("points", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("birthday", DataTypes.DateType, false, Metadata.empty())});

        List<Trip> list = new ArrayList<>();
        list.add(buildTrip());
        Dataset<Trip> dataset = sparkSession.createDataset(list, Encoders.bean(Trip.class));

        //System.out.println(dataset.schema());
        dataset.write().mode("overwrite").saveAsTable("trips");
        System.out.println("done");



    }

    private static Trip buildTrip() {
        String driverId = UUID.randomUUID().toString();
        String userId = UUID.randomUUID().toString();
        String tripId = UUID.randomUUID().toString();
        double startLatitude = initLatitude + RandomUtils.nextDouble(0, 0.001);
        double startLongitude = initLongitude + RandomUtils.nextDouble(0, 0.001);
        double endLatitude = initLatitude + RandomUtils.nextDouble(0, 0.001);
        double endLongitude = initLongitude + RandomUtils.nextDouble(0, 0.001);

        long orderTimestamp = System.currentTimeMillis() - daysBack(RandomUtils.nextLong(0, 90));
        long startTimestamp = minutesAfter(orderTimestamp, RandomUtils.nextLong(0, 10));
        long endTimestamp = minutesAfter(startTimestamp, RandomUtils.nextLong(0, 120));
        byte paymentType = RandomUtils.nextBoolean() ? (byte) 1 : (byte) 2;

        return new Trip(tripId, userId, driverId, startLatitude, startLongitude, endLatitude, endLongitude, orderTimestamp, startTimestamp, endTimestamp, paymentType);
    }

    public static long daysBack(long days) {
        return System.currentTimeMillis() - Duration.ofDays(days).toMillis();
    }

    public static long minutesAfter(long timestamp, long minutes) {
        return System.currentTimeMillis() + Duration.ofMinutes(minutes).toMillis();
    }


}

