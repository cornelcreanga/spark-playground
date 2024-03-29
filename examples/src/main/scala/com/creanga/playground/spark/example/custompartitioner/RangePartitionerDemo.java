package com.creanga.playground.spark.example.custompartitioner;

import com.creanga.playground.spark.example.partitioner.SyntheticRddProvider;
import com.creanga.playground.spark.util.FastRandom;
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
import java.nio.ByteBuffer;
import java.util.*;

import static org.apache.curator.shaded.com.google.common.primitives.UnsignedBytes.lexicographicalComparator;

public class RangePartitionerDemo {

//    public static void main(String[] args) throws IOException {
//        SparkConf conf = new SparkConf()
//                .setMaster("local[15]")
//                .setAppName("Partition demo")
//                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//                .set("spark.io.compression.codec", "lz4")
//                .set("spark.io.compression.lz4.blockSize", "256k")
//                .set("spark.rdd.compress", "true");
//        SparkContext sc = new SparkContext(conf);
//        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(sc);
//
//        int partitions = 40;
//        int reservedPartitions = 0;
//        int allocatablePartitions = partitions - reservedPartitions;
//        int values = 10000000;
//
//        List<Tuple3<byte[], Integer, Integer>> stats = new ArrayList<>();
//        try (InputStream in = CustomPartitionerDemo.class.getResourceAsStream("/stats.csv"); //cid, eventNo, eventsTotalsize, eventsTotalsize/eventNo
//             BufferedReader reader = new BufferedReader(new InputStreamReader(in))) {
//            String line;
//            while ((line = reader.readLine()) != null) {
//                String[] items = line.split("\t");
//                byte[] cid = asBytes(UUID.randomUUID());
//                stats.add(new Tuple3<>(cid, Integer.parseInt(items[0]), Integer.parseInt(items[1])));
//            }
//        } catch (Exception e) {
//            throw new RuntimeException("cannot create the synthetic rdd, an error appeared during parsing the frequency file", e);
//        }
//
//        SyntheticRddProvider rddProvider = new SyntheticRddProvider(jsc, stats, partitions, values, new HashMap<>());
//        JavaPairRDD<byte[], byte[]> pairRDD = rddProvider.buildRdd().mapToPair(t -> t);
//        pairRDD.persist(StorageLevel.MEMORY_ONLY());
//
//        //compute frequencies for all the items
//        //for each uuid we will compute cost = sum of all byte[]messages length
//        Map<byte[], Long> freqs = pairRDD.mapToPair(t -> new Tuple2<>(t._1, (long) t._2.length)).reduceByKeyLocally(Long::sum);
//        Map<byte[], Integer> saltingInfo = new HashMap<>();
//
//        long no = 0;
//        List<byte[]> keys = new ArrayList<>(freqs.keySet());
//        List<Long> keyCosts = new ArrayList<>(freqs.values());
//        double[] keysWeights = new double[keyCosts.size()];
//        for (long cost : keyCosts) {
//            no += cost;
//        }
//
//        long partitionCapacity = no / allocatablePartitions;
//        for (int i = 0; i < keyCosts.size(); i++) {
//            keysWeights[i] = (double) keyCosts.get(i) / partitionCapacity;
//            int salt = (int) Math.ceil(keysWeights[i] * 10);
//            if (salt > 1)
//                saltingInfo.put(keys.get(i), salt);
//        }
//        JavaPairRDD<byte[], byte[]> saltedPairRDD = pairRDD.mapToPair((t -> {
//            FastRandom random = new FastRandom();
//            Integer salt = saltingInfo.get(t._1);
//            if (salt != null) {
//                return new Tuple2<>(t._1 + "_" + random.nextInt(salt), t._2);
//            } else {
//                return t;
//            }
//        }));
//
//        Ordering<byte[]> ordering = Ordering$.MODULE$.comparatorToOrdering(lexicographicalComparator());
//        ClassTag<byte[]> classTag = ClassTag$.MODULE$.apply(byte[].class);
//        RangePartitioner<byte[], byte[]> partitioner = new RangePartitioner<>(partitions, saltedPairRDD.rdd(), true, ordering, classTag);
//
//        JavaPairRDD<byte[], byte[]> rangeRepartitionedRDD = saltedPairRDD.repartitionAndSortWithinPartitions(partitioner);
//        rangeRepartitionedRDD.count();
//
//        System.in.read();
//    }


}
