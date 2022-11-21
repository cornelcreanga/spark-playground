package com.creanga.playground.spark.example.custompartitioner;

import com.creanga.playground.spark.example.partitioner.SyntheticRddProvider;
import com.creanga.playground.spark.util.FastRandom;
import org.apache.spark.HashPartitioner;
import org.apache.spark.RangePartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
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


public class HashPartitionerDemo {

    public static void main(String[] args) throws IOException {
        SparkConf conf = new SparkConf()
                .setMaster("local[15]")
                .setAppName("Partition demo")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .set("spark.io.compression.codec", "lz4")
                .set("spark.io.compression.lz4.blockSize", "256k")
                .set("spark.rdd.compress", "true");
        SparkContext sc = new SparkContext(conf);
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(sc);

        int partitions = 20;
        int reservedPartitions = 0;
        int allocatablePartitions = partitions - reservedPartitions;
        int values = 4 * 1_000_000;

        List<Tuple3<UUID, Integer, Integer>> stats = new ArrayList<>();
        try (InputStream in = HashPartitionerDemo.class.getResourceAsStream("/stats.csv"); //cid, eventNo, eventsTotalsize, eventsTotalsize/eventNo
             BufferedReader reader = new BufferedReader(new InputStreamReader(in))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] items = line.split("\t");
                stats.add(new Tuple3<>(UUID.randomUUID(), Integer.parseInt(items[0]), Integer.parseInt(items[1])));
            }
        } catch (Exception e) {
            throw new RuntimeException("cannot create the synthetic rdd, an error appeared during parsing the frequency file", e);
        }

        SyntheticRddProvider rddProvider = new SyntheticRddProvider(jsc, stats, partitions, values, new HashMap<>());
        JavaPairRDD<String, byte[]> pairRDD = rddProvider.buildRdd().mapToPair(t -> new Tuple2<>(t._1.toString(), t._2));
        pairRDD.persist(StorageLevel.MEMORY_ONLY());

        //compute frequencies for all the items
        //for each uuid we will compute cost = sum of all byte[]messages length
        Map<String, Long> freqs = pairRDD.mapToPair(t -> new Tuple2<>(t._1, (long) t._2.length)).reduceByKeyLocally(Long::sum);
        Map<String, Integer> saltingInfo = new HashMap<>();

        long no = 0;
        List<String> keys = new ArrayList<>(freqs.keySet());
        List<Long> keyCosts = new ArrayList<>(freqs.values());
        double[] keysWeights = new double[keyCosts.size()];
        for (long cost : keyCosts) {
            no += cost;
        }

        long partitionCapacity = no / allocatablePartitions;
        for (int i = 0; i < keyCosts.size(); i++) {
            keysWeights[i] = (double) keyCosts.get(i) / partitionCapacity;
            int salt = (int) Math.ceil(keysWeights[i] * 10);
            if (salt > 1)
                saltingInfo.put(keys.get(i), salt);
        }
        JavaPairRDD<String, byte[]> saltedPairRDD = pairRDD.mapToPair((t -> {
            FastRandom random = new FastRandom();
            Integer salt = saltingInfo.get(t._1);
            if (salt != null) {
                return new Tuple2<>(t._1 + "_" + random.nextInt(salt), t._2);
            } else {
                return t;
            }
        }));

        JavaPairRDD<String, byte[]> hashRepartitionedRDD = saltedPairRDD.repartitionAndSortWithinPartitions(new HashPartitioner(partitions));

        hashRepartitionedRDD.count();
        Ordering<String> ordering = Ordering$.MODULE$.comparatorToOrdering(Comparator.<String>naturalOrder());
        ClassTag<String> classTag = ClassTag$.MODULE$.apply(String.class);
        RangePartitioner<String, byte[]> partitioner = new RangePartitioner<>(partitions, saltedPairRDD.rdd(), true, ordering, classTag);

        JavaPairRDD<String, byte[]> rangeRepartitionedRDD = saltedPairRDD.repartitionAndSortWithinPartitions(partitioner);
        rangeRepartitionedRDD.count();

        System.in.read();

    }

}
