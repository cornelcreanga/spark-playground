package com.creanga.playground.spark.example.custompartitioner;

import com.creanga.playground.spark.util.FastRandom;
import org.apache.commons.math3.distribution.EnumeratedDistribution;
import org.apache.commons.math3.util.Pair;

import java.io.Serializable;
import java.util.List;

public class PartitionDistribution implements Serializable {

    private static final FastRandom random = new FastRandom();

    private Integer singlePartition;
    private EnumeratedDistribution<Integer> enumeratedDistribution;

    public PartitionDistribution(Integer singlePartition) {
        this.singlePartition = singlePartition;
    }

    public PartitionDistribution(List<Pair<Integer, Double>> probabilities) {
        this.enumeratedDistribution = new EnumeratedDistribution<>(random, probabilities);
    }

    public int getPartition() {
        if (singlePartition != null) {
            return singlePartition;
        } else {
            return enumeratedDistribution.sample();
        }
    }

}
