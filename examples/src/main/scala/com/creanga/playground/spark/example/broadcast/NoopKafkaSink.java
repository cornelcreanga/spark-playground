package com.creanga.playground.spark.example.broadcast;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.concurrent.Future;

public class NoopKafkaSink<K, V> implements KafkaSink<K, V> {
    @Override
    public void flush() {

    }

    @Override
    public void close() {

    }

    @Override
    public Future<RecordMetadata> send(String topic, V value, Callback callback) {
        return null;
    }

    @Override
    public void send(String topic, V value) {

    }
}
