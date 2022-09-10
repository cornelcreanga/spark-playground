package com.creanga.playground.spark.example.custompartitioner;

import java.io.Serializable;

public class PartitionInfo implements Serializable {

    private int partition;
    private double probability;

    public PartitionInfo(int partition, double probability) {
        this.partition = partition;
        this.probability = probability;
    }

    public int getPartition() {
        return partition;
    }

    public double getProbability() {
        return probability;
    }

}
