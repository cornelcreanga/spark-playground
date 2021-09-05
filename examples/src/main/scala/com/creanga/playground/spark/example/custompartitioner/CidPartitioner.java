package com.creanga.playground.spark.example.custompartitioner;

import org.apache.spark.Partitioner;

public class CidPartitioner extends Partitioner {

    private int partitions;
    private long maxPartitionSize;

    private Long[] partitionLoad;
    private int counter = 0;

    public CidPartitioner(int partitions, long maxPartitionSize) {
        this.partitions = partitions;
        this.maxPartitionSize = maxPartitionSize;
        partitionLoad = new Long[partitions];
        for (int i = 0; i < partitions; i++) {
             partitionLoad[i] = 0L;
        }
    }

    @Override
    public int numPartitions() {
        return partitions;
    }

    @Override
    public int getPartition(Object key) {

        Location location = (Location)key;
        if (partitionLoad[counter] > maxPartitionSize) {
            counter = counter + 1;
        }
        partitionLoad[counter] =  partitionLoad[counter] + location.filesTotalSize;
        int assignedPartition = counter;
       // System.out.println(assignedPartition + "-"+location + "-" +counter);



        return assignedPartition;
    }
}
