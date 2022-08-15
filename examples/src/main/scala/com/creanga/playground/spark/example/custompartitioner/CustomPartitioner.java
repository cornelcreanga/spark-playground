package com.creanga.playground.spark.example.custompartitioner;

import com.creanga.playground.spark.util.FastRandom;
import org.apache.commons.math3.distribution.EnumeratedDistribution;
import org.apache.spark.Partitioner;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Map;

public class CustomPartitioner extends Partitioner implements Serializable {

    private final int partitions;
    private final Map<String, Object> distribution;

    public CustomPartitioner(Broadcast<Map<String, Object>> distributionBroadcast, int partitions) {
        this.distribution = distributionBroadcast.getValue();
        this.partitions = partitions;
    }

    @Override
    public int numPartitions() {
        return partitions;
    }

    @Override
    public int getPartition(Object key) {
        Object partition = distribution.get((String)key);
        if (partition instanceof Integer){
            return (Integer)partition;
        }else{
            return (Integer)((EnumeratedDistribution)partition).sample();
        }
    }
}
