package com.creanga.playground.spark.example.custompartitioner;

import java.io.Serializable;

public interface CostFunction<V> extends Serializable {
    long computeCost(V data);
}
