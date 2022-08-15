package com.creanga.playground.spark.util;

import org.apache.spark.Partition;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class RDDGeneratorPartition<T> implements Partition {

    private final int index;
    private final int numValues;
    private final Map<String, Object> context;
    private final RecordGenerator<T> generatorFunction;

    public RDDGeneratorPartition(int index, int numValues, Map<String, Object> context, RecordGenerator<T> generatorFunction) {
        this.index = index;
        this.numValues = numValues;
        this.context = context;
        this.generatorFunction = generatorFunction;
        context.put("partitionIndex", "" + index);
    }

    @Override
    public int index() {
        return index;
    }

    public List<T> values() {
        return generatorFunction.generate(context, numValues);
    }

}


