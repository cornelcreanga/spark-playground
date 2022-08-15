package com.creanga.playground.spark.example.custompartitioner;

import com.creanga.playground.spark.util.FastRandom;
import com.creanga.playground.spark.util.RDDGenerator;
import com.creanga.playground.spark.util.RecordGenerator;
import org.apache.commons.math3.distribution.EnumeratedDistribution;
import org.apache.commons.math3.util.Pair;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;
import scala.Tuple3;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class SyntheticRddProvider implements Serializable {


    private final transient JavaSparkContext jsc;
    private final int partitions;
    private final int values;
    private final Map<String, Object> context;
    private final Broadcast<EnumeratedDistribution<String>> distributionBroadcast;
    private final Broadcast<Map<String, Integer>> cidToEventSizeBroadcast;

    public SyntheticRddProvider(JavaSparkContext jsc, List<Tuple3<String, Integer, Integer>> stats, int partitions, int values, Map<String, Object> context) {
        this.jsc = jsc;
        this.partitions = partitions;
        this.values = values;
        this.context = context;

        List<Pair<String, Double>> distributionList = new ArrayList<>();
        Map<String, Integer> cidToEventSize = new HashMap<>();
        for (Tuple3<String, Integer, Integer> t : stats) {
            distributionList.add(new Pair<>(t._1(), (double) t._2() * t._3()));
            cidToEventSize.put(t._1(), t._3());
        }

        EnumeratedDistribution<String> distribution = new EnumeratedDistribution<>(distributionList);
        distributionBroadcast = jsc.broadcast(distribution);
        cidToEventSizeBroadcast = jsc.broadcast(cidToEventSize);
    }


    public JavaRDD<Tuple2<String, byte[]>> buildKafkaRdd() {

        RecordGenerator<Tuple2<String, byte[]>> recordGenerator = (context, itemNumber) -> {
            //we dont care about item offset/partition for the moment
            FastRandom fastRandom = new FastRandom();

            EnumeratedDistribution<String> distribution = distributionBroadcast.getValue();
            Map<String, Integer> cidToEventSize = cidToEventSizeBroadcast.getValue();
            List<Tuple2<String, byte[]>> list = new ArrayList<>(itemNumber);
            for (int i = 0; i < itemNumber; i++) {
                String cid = distribution.sample();
                byte[] event = new byte[cidToEventSize.get(cid)];
                fastRandom.nextBytes(event);
                list.add(new Tuple2<>(cid, event));
            }
            return list;
        };

        Class clazz = ConsumerRecord.class;
        RDDGenerator<Tuple2<String, byte[]>> rdd = RDDGenerator.of(jsc.sc(),
                partitions,
                values,
                context,
                recordGenerator,
                clazz
        );

        return rdd.toJavaRDD();
    }

}
