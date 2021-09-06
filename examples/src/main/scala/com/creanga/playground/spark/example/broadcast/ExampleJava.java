package com.creanga.playground.spark.example.broadcast;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Properties;

public class ExampleJava {

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder()
                .master("local[4]")
                .appName("Spark streaming")
                .getOrCreate();

        ClassTag<KafkaSinkImpl<byte[], byte[]>> uClassTag = ClassTag$.MODULE$.apply(KafkaSinkImpl.class);
        SparkContext sc = sparkSession.sparkContext();
        JavaSparkContext jsc = new JavaSparkContext(sc);
        ArrayList<String> paths = new ArrayList<>();
        paths.add("1");
        paths.add("2");
        paths.add("3");
        paths.add("4");
        paths.add("5");

        Properties config = new Properties();
        config.put("bootstrap.servers", "localhost:9093");

        config.put("acks", "all");
        config.put("retries", 0);
        config.put("batch.size", 16384);
        config.put("linger.ms", 1);
        config.put("buffer.memory", 33554432);
        config.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        config.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

        KafkaSinkImpl<byte[], byte[]> kafkaSinkImpl = KafkaSinkImpl.getInstance(config);

        Broadcast<KafkaSinkImpl<byte[], byte[]>> broadcast = sc.broadcast(kafkaSinkImpl, uClassTag);

        JavaRDD<String> rdd = jsc.parallelize(paths);
        System.out.println(rdd.map(new Function<String, Object>() {
            @Override
            public Object call(String v1) throws Exception {
                broadcast.value().send("cucu","hophop".getBytes(StandardCharsets.UTF_8));
                //kafkaSink.send("",null);
                return v1;
            }
        }).count());





    }
}
