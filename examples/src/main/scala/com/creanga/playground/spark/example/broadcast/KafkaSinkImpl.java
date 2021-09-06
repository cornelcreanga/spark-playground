package com.creanga.playground.spark.example.broadcast;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.Future;
import java.util.function.Function;

public class KafkaSinkImpl<K, V> implements KafkaSink<K, V> {

    private transient static KafkaSinkImpl instance;
    private transient final KafkaProducer<K, V> producer;

    private KafkaSinkImpl(Function<Properties, KafkaProducer<K, V>> createProducer, Properties config) {
        this.producer = createProducer.apply(config);
    }

    public void flush() {
        producer.flush();
    }

    public void close() {
        producer.close();
    }

    public Future<RecordMetadata> send(String topic, V value, Callback callback) {
        return producer.send(new ProducerRecord<>(topic, value), callback);
    }

    public void send(String topic, V value) {
        producer.send(new ProducerRecord<>(topic, value), null);
    }

    public static synchronized <K, V> KafkaSinkImpl<K, V> getInstance(Properties config) {
        if (instance == null) {
            instance = new KafkaSinkImpl<K, V>(properties -> new KafkaProducer<K, V>(config), config);
        }
        return (KafkaSinkImpl<K, V>)instance;
    }
}
