package com.creanga.playground.spark.example.custompartitioner;

import com.creanga.playground.spark.util.FastRandom;
import org.apache.commons.math3.distribution.EnumeratedDistribution;
import org.apache.commons.math3.util.Pair;

import java.io.Serializable;
import java.util.List;

public class PartitionDistribution implements Serializable {

    private static FastRandom random = new FastRandom();

    private Integer firstPartition;
    private Integer secondPartition;
    private Double firstProbability;
    private Double secondProbability;
    private EnumeratedDistribution<Integer> enumeratedDistribution;

    public PartitionDistribution(Integer firstPartition) {
        this.firstPartition = firstPartition;
    }

    public PartitionDistribution(Integer firstPartition, Integer secondPartition, Double firstProbability, Double secondProbability) {
        this.firstPartition = firstPartition;
        this.secondPartition = secondPartition;
        this.firstProbability = firstProbability;
        this.secondProbability = secondProbability;
    }

    public PartitionDistribution(List<Pair<Integer, Double>> probabilities) {
        this.enumeratedDistribution = new EnumeratedDistribution<>(random, probabilities);
    }

    public int getPartition(){
        if (firstPartition !=null){
            if (secondPartition == null){
                return firstPartition;
            }else{
                return probabilities(firstProbability, secondProbability, firstPartition, secondPartition);
            }
        }else{
            return enumeratedDistribution.sample();
        }
    }

    private int probabilities(double firstProbability, double secondProbability, int firstPartition, int secondPartition) {
        double r = random.nextDouble() / 1;
        if (firstProbability > secondProbability) {
            return (r > secondProbability / firstProbability) ? firstPartition : secondPartition;
        } else {
            return (r > firstProbability / secondProbability) ? secondPartition : firstPartition;
        }
    }

}
