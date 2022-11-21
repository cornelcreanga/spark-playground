package com.creanga.playground.spark.example.partitioner;

import com.creanga.playground.spark.util.FastRandom;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.*;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;
import scala.Tuple3;
import scala.math.Ordering;
import scala.math.Ordering$;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.*;


public class PartitioningExample {

    public static void main(String[] args) throws IOException {
        SparkConf conf = new SparkConf()
                .setMaster("local[15]")
                .setAppName("Partition demo")
                .set("spark.driver.memoryOverheadFactor", "0.05")
                .set("spark.memory.fraction", "0.8");
        SparkContext sc = new SparkContext(conf);
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(sc);
        long t1, t2;
        int partitions = 40;
        int reservedPartitions = 0;
        int allocatablePartitions = partitions - reservedPartitions;
        int values = 10_000_000;

        List<Tuple3<UUID, Integer, Integer>> stats = new ArrayList<>();
        try (InputStream in = PartitioningExample.class.getResourceAsStream("/stats.csv"); //cid, eventNo, eventsTotalsize, eventsTotalsize/eventNo
             BufferedReader reader = new BufferedReader(new InputStreamReader(in))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] items = line.split("\t");
                UUID cid = UUID.randomUUID();
                stats.add(new Tuple3<>(cid, Integer.parseInt(items[0]), Integer.parseInt(items[1])));
            }
        } catch (Exception e) {
            throw new RuntimeException("cannot create the synthetic rdd, an error appeared during parsing the frequency file", e);
        }

        SyntheticRddProvider rddProvider = new SyntheticRddProvider(jsc, stats, partitions, values, new HashMap<>());
        JavaPairRDD<UUID, byte[]> pairRDD = rddProvider.buildRdd().mapToPair(stringTuple2 -> stringTuple2);
        pairRDD.persist(StorageLevel.MEMORY_ONLY());

        //compute frequencies for all the items
        //for each uuid we will compute cost = sum of all byte[]messages length
        Map<UUID, Long> freqs = pairRDD.mapToPair(t -> new Tuple2<>(t._1, (long) t._2.length)).reduceByKeyLocally(Long::sum);
        System.out.println("total keys " + freqs.size());
        Map<UUID, Integer> saltingInfo = new HashMap<>();
        long no = 0;
        List<UUID> keys = new ArrayList<>(freqs.keySet());
        List<Long> keyCosts = new ArrayList<>(freqs.values());
        double[] keysWeights = new double[keyCosts.size()];
        for (long cost : keyCosts) {
            no += cost;
        }

        //hash partitioner
        long partitionCapacity = no / allocatablePartitions;
        for (int i = 0; i < keyCosts.size(); i++) {
            keysWeights[i] = (double) keyCosts.get(i) / partitionCapacity;
            int salt = (int) Math.ceil(keysWeights[i] * 10);
            if (salt > 1)
                saltingInfo.put(keys.get(i), salt);
        }
        JavaPairRDD<String, byte[]> saltedPairRDD = pairRDD.mapToPair(t -> {
            FastRandom random = new FastRandom();
            Integer salt = saltingInfo.get(t._1);
            if (salt != null) {
                return new Tuple2<>(t._1.toString() + "_" + random.nextInt(salt), t._2);
            } else {
                return new Tuple2<>(t._1.toString(), t._2);
            }
        });


        JavaPairRDD<String, byte[]> hashRepartitionedRDD = saltedPairRDD.repartitionAndSortWithinPartitions(new HashPartitioner(partitions));
        JavaPairRDD<String, byte[]> unsaltedHashedRDD = getUnsaltedRDD(hashRepartitionedRDD);
        Map<Integer, Map<String, Integer>> distribution = computeDistribution(unsaltedHashedRDD);

        long distinctGroups = getDistinctGroups(distribution);
        System.out.println("hash distinct groups " + distinctGroups);

        Ordering<String> ordering = Ordering$.MODULE$.comparatorToOrdering(Comparator.<String>naturalOrder());
        ClassTag<String> classTag = ClassTag$.MODULE$.apply(String.class);
        RangePartitioner<String, byte[]> partitioner = new RangePartitioner<>(partitions, saltedPairRDD.rdd(), true, ordering, classTag);

        JavaPairRDD<String, byte[]> rangeRepartitionedRDD = saltedPairRDD.repartitionAndSortWithinPartitions(partitioner);
        JavaPairRDD<String, byte[]> unsaltedRepartitionedRDD = getUnsaltedRDD(rangeRepartitionedRDD);
        distribution = computeDistribution(unsaltedRepartitionedRDD);
        distinctGroups = getDistinctGroups(distribution);
        System.out.println("range distinct groups " + distinctGroups);

    }

    private static JavaPairRDD<String, byte[]> getUnsaltedRDD(JavaPairRDD<String, byte[]> rdd) {
        return rdd.mapToPair(t -> {
            String key = t._1;
            if (!key.contains("_")) {
                return t;
            } else {
                return new Tuple2<>(StringUtils.substringBefore(key, "_"), t._2);
            }
        });
    }

    private static long getDistinctGroups(Map<Integer, Map<String, Integer>> distribution) {
        long distinctGroups = 0;
        Collection<Map<String, Integer>> distributionValues = distribution.values();
        for (Iterator<Map<String, Integer>> iterator = distributionValues.iterator(); iterator.hasNext(); ) {
            Map<String, Integer> next = iterator.next();
            distinctGroups += next.size();
        }
        return distinctGroups;
    }

    private static Map<Integer, Map<String, Integer>> computeDistribution(JavaPairRDD<String, byte[]> rdd){
        return rdd.mapPartitionsToPair(it -> {
            Map<String, Integer> f = new HashMap<>();
            it.forEachRemaining(t -> {
                Integer previous = f.get(t._1);
                if (previous == null) {
                    f.put(t._1, 1);
                } else {
                    f.put(t._1, previous + 1);
                }
            });
            return Collections.singletonList(new Tuple2<>(TaskContext.getPartitionId(), f)).iterator();
        }).collectAsMap();
    }


}
