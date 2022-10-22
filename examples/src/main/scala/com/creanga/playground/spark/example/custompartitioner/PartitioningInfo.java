package com.creanga.playground.spark.example.custompartitioner;

import com.google.common.collect.ImmutableMap;

import java.util.Map;

public class PartitioningInfo {

    private final int partitionNo;
    private final Map<String, PartitionDistribution> distributionMap;

    public PartitioningInfo(int partitionNo, Map<String, PartitionDistribution> distributionMap) {
        this.partitionNo = partitionNo;
        this.distributionMap = distributionMap;
    }

    public int getPartitionNo() {
        return partitionNo;
    }

    public Map<String, PartitionDistribution> getDistributionMap() {
        return ImmutableMap.copyOf(distributionMap);
    }
}
