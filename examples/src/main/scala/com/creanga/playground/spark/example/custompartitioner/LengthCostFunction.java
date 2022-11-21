package com.creanga.playground.spark.example.custompartitioner;

public class LengthCostFunction implements CostFunction<byte[]> {

    @Override
    public long computeCost(byte[] data) {
        return data.length;
    }
}
