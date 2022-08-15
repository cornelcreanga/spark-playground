package com.creanga.playground.spark.example.custompartitioner;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.spark.RangePartitioner;
import org.apache.spark.SparkContext;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import scala.math.Ordering;
import scala.math.Ordering$;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

import java.util.*;

public class RangePartitionerDemo {

    public static List<Tuple2<String, String>> generateList(Map<String, Integer> frequencies) {


        List<Tuple2<String, String>> result = new ArrayList<>();
        Set<String> keys = frequencies.keySet();

        for (String next : keys) {
            for (int j = 0; j < 100 * frequencies.get(next); j++) {
                result.add(new Tuple2<>(next, RandomStringUtils.random(128, true, false)));
            }
        }

        Collections.shuffle(result);
        return result;
    }

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder()
                .master("local[4]")
                .appName("Spark streaming")
                .getOrCreate();
        SparkContext sc = sparkSession.sparkContext();
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(sc);
        Map<String, Integer> frequencies = ImmutableMap.of("a", 100, "b", 40, "c", 10, "d", 10);

        List<Tuple2<String, String>> list = generateList(frequencies);
        int partitions = 4;
        System.out.println("list length:" + list.size());
        JavaPairRDD<String, String> rdd = jsc.parallelizePairs(list).mapToPair(tuple -> {
            if (!frequencies.containsKey(tuple._1())) {
                return tuple;
            }
            String key = tuple._1() + "_" + (int) (Math.random() * frequencies.get(tuple._1()));
            return new Tuple2<>(key, tuple._2());
        });

        //JavaPairRDD<String, String> rddUnskewed = rdd.mapToPair(t -> new Tuple2<>(t._1 + "_" + rand.nextInt(skewFactor), t._2));

        Ordering<String> ordering = Ordering$.MODULE$.comparatorToOrdering(Comparator.<String>naturalOrder());
        ClassTag<String> classTag = ClassTag$.MODULE$.apply(String.class);
        RangePartitioner<String, String> partitioner = new RangePartitioner<>(partitions, rdd.rdd(), true, ordering, classTag);
//        JavaPairRDD<String, String> rddRangePartitioned = rdd.partitionBy(partitioner);
        JavaPairRDD<String, String> rddRangePartitioned = rdd.repartitionAndSortWithinPartitions(partitioner);


        JavaPairRDD<String, String> rddRangeUnmapped = rddRangePartitioned.mapToPair(tuple -> {
            String key = tuple._1();
            int index = key.indexOf("_");
            if (index == -1)
                return tuple;
            return new Tuple2<>(key.substring(0, index), tuple._2());
        });


        //rddRangeUnmapped.so

        rddRangeUnmapped.foreachPartition((VoidFunction<Iterator<Tuple2<String, String>>>) tuple2Iterator -> {
            int count = 0;
            int partId = TaskContext.getPartitionId();
            while (tuple2Iterator.hasNext()) {
                count++;
                Tuple2<String, String> next = tuple2Iterator.next();
//                if (partId == 2) {
//                    System.out.println(next._1);
//                }
                //System.out.println(TaskContext.getPartitionId() + " - " + next._1);
            }
            System.out.println(TaskContext.getPartitionId() + " - " + count);

        });
    }
}
