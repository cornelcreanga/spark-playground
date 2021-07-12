package com.creanga.playground.spark.util;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;

import java.util.Properties;

public class KafkaMessageProducer {

    public static KafkaProducer<byte[],byte[]> kafkaProducer = producer();

    private static KafkaProducer<byte[],byte[]> producer(){
        Properties props = new Properties();

        //Assign localhost id
        props.put("bootstrap.servers", "localhost:9093");

        props.put("acks", "1");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

        return new KafkaProducer<>(props);
    }



}
