package com.creanga.playground.spark.example.custompartitioner;

import org.apache.spark.Partitioner;
import org.apache.spark.broadcast.Broadcast;

import java.io.Serializable;
import java.util.Map;

public class CustomPartitioner extends Partitioner implements Serializable {

    private final int partitions;
    private final int reservedPartitions;
    private final Map<String, PartitionDistribution> distribution;

    public CustomPartitioner(Broadcast<Map<String, PartitionDistribution>> distributionBroadcast, int partitions, int reservedPartitions) {
        this.distribution = distributionBroadcast.getValue();
        this.partitions = partitions;
        this.reservedPartitions = reservedPartitions;
    }

    @Override
    public int numPartitions() {
        return partitions;
    }

    @Override
    public int getPartition(Object partitionKey) {
        String key = (String) partitionKey;
        PartitionDistribution partitionDistribution = distribution.get(key);
        if (partitionDistribution != null) {
            return partitionDistribution.getPartition();
        } else {
            return (partitions - reservedPartitions) + key.hashCode() % reservedPartitions;
        }
    }
}
