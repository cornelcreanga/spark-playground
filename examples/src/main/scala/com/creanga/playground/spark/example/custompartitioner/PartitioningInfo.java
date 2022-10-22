package com.creanga.playground.spark.example.custompartitioner;

import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.UUID;

public class PartitioningInfo {

    private final int partitionNo;
    private final Map<UUID, PartitionDistribution> distributionMap;

    public PartitioningInfo(int partitionNo, Map<UUID, PartitionDistribution> distributionMap) {
        this.partitionNo = partitionNo;
        this.distributionMap = distributionMap;
    }

    public int getPartitionNo() {
        return partitionNo;
    }

    public Map<UUID, PartitionDistribution> getDistributionMap() {
        return ImmutableMap.copyOf(distributionMap);
    }
}
