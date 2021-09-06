package com.creanga.playground.spark.example.broadcast;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.Serializable;
import java.util.concurrent.Future;

public interface KafkaSink<K, V> extends Serializable {

    void flush();

    void close();

    Future<RecordMetadata> send(String topic, V value, Callback callback);

    void send(String topic, V value);

}
