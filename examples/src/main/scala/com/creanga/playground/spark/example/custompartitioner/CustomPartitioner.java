package com.creanga.playground.spark.example.custompartitioner;

import org.apache.commons.math3.util.Pair;
import org.apache.spark.Partitioner;
import org.apache.spark.broadcast.Broadcast;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

public class CustomPartitioner extends Partitioner implements Serializable {

    private final int partitions;
    private final int reservedPartitions;
    private final Map<String, PartitionDistribution> distribution;

    public CustomPartitioner(Broadcast<Map<String, PartitionDistribution>> distributionBroadcast, int partitions, int reservedPartitions) {
        this.distribution = distributionBroadcast.getValue();
        this.partitions = partitions;
        this.reservedPartitions = reservedPartitions;
    }

    public static Map<String, PartitionDistribution> computePartitionDistribution(Map<String, Long> keysToCost, int allocatablePartitions){

        long no = 0;
        List<String> keys = new ArrayList<>(keysToCost.keySet());
        List<Long> keyCosts = new ArrayList<>(keysToCost.values());
        double[] keysWeights = new double[keyCosts.size()];
        for (long cost : keyCosts) {
            no += cost;
        }

        long partitionCapacity = no / allocatablePartitions;
        for (int i = 0; i < keyCosts.size(); i++) {
            keysWeights[i] = (double) keyCosts.get(i) / partitionCapacity;
        }

        double[] partitionAvailabilities = new double[allocatablePartitions];
        Arrays.fill(partitionAvailabilities, 1);
        Map<String, List<PartitionInfo>> keyPartitionProbabilities = new HashMap<>();
        for (int i = 0; i < keyCosts.size(); i++) {
            double keyPartition = keysWeights[i];
            List<PartitionInfo> list = new ArrayList<>();
            for (int j = 0; j < partitionAvailabilities.length; j++) {
                if (keyPartition == 0)
                    break;

                double partitionAvailability = partitionAvailabilities[j];
                if (partitionAvailability == 0)
                    continue;
                if (keyPartition >= partitionAvailability) {
                    list.add(new PartitionInfo(j, partitionAvailability));
                    partitionAvailabilities[j] = 0;
                    keyPartition -= partitionAvailability;
                } else {
                    list.add(new PartitionInfo(j, keyPartition));
                    partitionAvailabilities[j] -= keyPartition;
                    keyPartition = 0;
                }
            }
            if (!list.isEmpty()) {//last element might be left out due to double precision loss
                keyPartitionProbabilities.put(keys.get(i), list);
            } else {
                keyPartitionProbabilities.put(keys.get(i), Collections.singletonList(new PartitionInfo(partitionAvailabilities.length - 1, 1d)));
            }
        }

        Map<String, PartitionDistribution> distributionMap = new HashMap<>();
        Set<String> uuids = keyPartitionProbabilities.keySet();

        for (String uuid : uuids) {
            List<PartitionInfo> list = keyPartitionProbabilities.get(uuid);
            //if we have 1 partition there is no need to build a heavyweight EnumeratedDistribution
            if (list.size() == 1) {
                distributionMap.put(uuid, new PartitionDistribution(list.get(0).getPartition()));
            } else {
                List<Pair<Integer, Double>> pairs = list.stream().map(t -> new Pair<>(t.getPartition(), t.getProbability())).collect(Collectors.toList());
                distributionMap.put(uuid, new PartitionDistribution(pairs));
            }
        }
        return distributionMap;

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
            if (reservedPartitions == 0){
                throw new RuntimeException("no reserved partitions are allocated, can't repartition the key " + partitionKey);
            }
            return (partitions - reservedPartitions) + key.hashCode() % reservedPartitions;
        }
    }
}
