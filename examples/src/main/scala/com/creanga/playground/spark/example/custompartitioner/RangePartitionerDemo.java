package com.creanga.playground.spark.example.custompartitioner;

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

    public static List<Tuple2<String, String>> generateList() {

        List<Tuple2<String, String>> result = new ArrayList<>();
        String uuid = UUID.randomUUID().toString();
        for (int i = 0; i < 2100; i++) {
            result.add(new Tuple2<>("aaa", RandomStringUtils.random(128, true, false)));
        }
        uuid = UUID.randomUUID().toString();
        for (int i = 0; i < 50; i++) {
            result.add(new Tuple2<>("bbb", RandomStringUtils.random(128, true, false)));
        }
        for (int i = 0; i < 30; i++) {
            result.add(new Tuple2<>("ccc", RandomStringUtils.random(128, true, false)));
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
        List<Tuple2<String, String>> list = generateList();
        int partitions = 4;
        System.out.println("list length:" + list.size());
        JavaPairRDD<String, String> rdd = jsc.parallelizePairs(list);

        //JavaPairRDD<String, String> rddUnskewed = rdd.mapToPair(t -> new Tuple2<>(t._1 + "_" + rand.nextInt(skewFactor), t._2));

        Ordering<String> ordering = Ordering$.MODULE$.comparatorToOrdering(Comparator.<String>naturalOrder());
        ClassTag<String> classTag = ClassTag$.MODULE$.apply(String.class);
        RangePartitioner<String, String> partitioner = new RangePartitioner<>(partitions, rdd.rdd(), true, ordering, classTag);
        JavaPairRDD<String, String> rddRangePartitioned = rdd.partitionBy(partitioner);

        rddRangePartitioned.foreachPartition((VoidFunction<Iterator<Tuple2<String, String>>>) tuple2Iterator -> {
            int count = 0;
            while (tuple2Iterator.hasNext()) {
                count ++;
                Tuple2<String, String> next = tuple2Iterator.next();
                //System.out.println(TaskContext.getPartitionId() + " - " + next._1);
            }
            System.out.println(TaskContext.getPartitionId() + " - " + count);

        });
    }
}
