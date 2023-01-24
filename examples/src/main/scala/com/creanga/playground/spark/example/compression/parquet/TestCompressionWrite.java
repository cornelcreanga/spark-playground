package com.creanga.playground.spark.example.compression.parquet;

import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.storage.StorageLevel;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.ProtocolException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;

import static com.creanga.playground.spark.util.IOUtils.alternatePaths;

public class TestCompressionWrite {

    public static void main(String[] args) throws Exception {

        SparkSession sparkSession = SparkSession.builder()
                .master("local[8]")
                .appName("Spark streaming")
//                .config("parquet.block.size",128 * 1024 * 1024)
//                .config("parquet.page.size",4 * 1024 * 1024)
                .config("parquet.enable.dictionary#nucleotideexpression", false)
                .config("parquet.enable.dictionary#nucleotidechange", false)
//                .config("parquet.enable.dictionary#column.path", false)
//                .config("parquet.compression.codec.zstd.level","10")
//                .config("io.compression.codec.lz4.use.lz4hc","true")
                .getOrCreate();

        SparkContext sc = sparkSession.sparkContext();

        if (args.length != 1) {
            System.out.println("need a local path as an argument");
            System.exit(1);
        }
        String path = args[0];
        System.out.println("Using " + path + " as a local path");

        long t1, t2;
        /**
         * https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2022-01.parquet
         * https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2022-02.parquet
         * https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2022-03.parquet
         * https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2022-04.parquet
         * https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2022-05.parquet
         * https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2022-06.parquet
         * https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2022-07.parquet
         * https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2022-08.parquet
         * https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2021-01.parquet
         * https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2021-02.parquet
         * https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2021-03.parquet
         * https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2021-04.parquet
         * https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2021-05.parquet
         * https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2021-06.parquet
         * https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2021-07.parquet
         * https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2021-08.parquet
         * https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2021-09.parquet
         * https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2021-10.parquet
         * https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2021-11.parquet
         * https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2021-12.parquet
         */

        String localPath = downloadFile("https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2022-01.parquet","fhvhv_tripdata_2022-01.parquet");
        Dataset<Row> ds = sparkSession.read().parquet(localPath).coalesce(1);
//        Dataset<Row> ds = sparkSession.read().parquet("/home/cornel/parquet/").coalesce(1);
        ds.persist(StorageLevel.MEMORY_ONLY());
        ds.printSchema();
        ds.show(1, false);
//        ds.createOrReplaceTempView("data");
//        Dataset<Row> namesDF = sparkSession.sql("SELECT  count(*),count (distinct nucleotideexpression),count (distinct nucleotidechange) FROM data");
//        namesDF.show();
//
//        System.out.printf("rows %s \n", ds.count());

        t1 = System.currentTimeMillis();
        ds.
                write().
                mode(SaveMode.Overwrite).
                option("compression", "none").
                parquet(path + "/none");
        t2 = System.currentTimeMillis();

        System.out.printf("write time no compression %s size %s \n", (t2 - t1), FileUtils.sizeOfDirectory(new File(path + "/none")));

        t1 = System.currentTimeMillis();
        ds.
                write().
                mode(SaveMode.Overwrite).
                option("compression", "zstd").
                parquet(path + "/zstd");
        t2 = System.currentTimeMillis();
        System.out.printf("write time zstd %s size %s \n", (t2 - t1), FileUtils.sizeOfDirectory(new File(path + "/zstd")));

        t1 = System.currentTimeMillis();
        ds.
                write().
                mode(SaveMode.Overwrite).
                option("compression", "gzip").
                parquet(path + "/gzip");
        t2 = System.currentTimeMillis();
        System.out.printf("write time gzip %s size %s \n", (t2 - t1), FileUtils.sizeOfDirectory(new File(path + "/gzip")));

        t1 = System.currentTimeMillis();
        ds.
                write().
                mode(SaveMode.Overwrite).
                option("compression", "snappy").
                parquet(path + "/snappy");
        t2 = System.currentTimeMillis();
        System.out.printf("write time snappy %s size %s \n", (t2 - t1), FileUtils.sizeOfDirectory(new File(path + "/snappy")));

        t1 = System.currentTimeMillis();
        ds.
                write().
                mode(SaveMode.Overwrite).
                option("compression", "lz4").
                parquet(path + "/lz4");
        t2 = System.currentTimeMillis();
        System.out.printf("write time lz4 %s size %s \n", (t2 - t1), FileUtils.sizeOfDirectory(new File(path + "/lz4")));

    }

    private static String downloadFile(String uri, String name) throws Exception {
        String tmpdir = System.getProperty("java.io.tmpdir");
        String localPath = tmpdir+"/" + name;
        File file = new File(localPath);
        if (file.exists()){
            System.out.println("File found in " + localPath);
        }else if (uri.startsWith("http")){
            URL url = new URL(uri);
            HttpURLConnection con = (HttpURLConnection) url.openConnection();
            con.setRequestMethod("GET");

            try (InputStream inputStream = con.getInputStream(); OutputStream targetStream = Files.newOutputStream(Paths.get(localPath))) {
               // inputStream.transferTo(targetStream);
            }
        }else if (uri.startsWith("s3")){

        }
        return localPath;
    }


}
