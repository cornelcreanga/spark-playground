package com.creanga.playground.spark.example.custompartitioner;

import com.creanga.playground.spark.example.partitioner.SyntheticRddProvider;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.util.SizeEstimator;
import scala.Tuple2;
import scala.Tuple3;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.*;

import static com.creanga.playground.spark.example.custompartitioner.CustomPartitioner.computePartitionDistribution;
import static com.creanga.playground.spark.util.IOUtils.getResourceFileAsStream;

public class CustomPartitionerDemo {

    String path = "/tmp/data";

    public static List<Tuple2<String, Long>> generateList() throws IOException {
        List<String> lines = getResourceFileAsStream("stats.csv");
        List<Tuple2<String, Long>> list = new ArrayList<>();

        for (String line : lines) {
            String[] items = line.split("\t");
            list.add(new Tuple2<>(UUID.randomUUID().toString(), Long.parseLong(items[0]) * Long.parseLong(items[1])));
        }
        return list;
    }


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
        long t1, t2;
        long partitionCapacity = 300_000_000;
        int partitions = 40;
        int reservedPartitions = 0;

        int values = 10_000_000;


        List<Tuple3<UUID, Integer, Integer>> stats = new ArrayList<>();
        try (InputStream in = CustomPartitionerDemo.class.getResourceAsStream("/stats.csv"); //cid, eventNo, eventsTotalsize, eventsTotalsize/eventNo
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
        JavaPairRDD<UUID, byte[]> pairRDD = rddProvider.buildRdd().mapToPair(t -> t);
        pairRDD.persist(StorageLevel.MEMORY_ONLY());

        //compute frequencies for all the items
        //for each uuid we will compute cost = sum of all byte[]messages length
        Map<UUID, Long> freqs = pairRDD.mapToPair(t -> new Tuple2<>(t._1, (long) t._2.length)).reduceByKeyLocally(Long::sum);

        PartitioningInfo partitioningInfo = computePartitionDistribution(freqs, partitionCapacity);
        Map<UUID, PartitionDistribution> distributionMap = partitioningInfo.getDistributionMap();

        System.out.println("Distribution map size "+SizeEstimator.estimate(distributionMap));

        System.out.println("No of partitions " + partitioningInfo.getPartitionNo());

        Broadcast<Map<UUID, PartitionDistribution>> distributionBroadcast = jsc.broadcast(distributionMap);

        t1 = System.currentTimeMillis();
        JavaPairRDD<UUID, byte[]> repartitionedRDD = pairRDD.repartitionAndSortWithinPartitions(new CustomPartitioner(distributionBroadcast, partitioningInfo.getPartitionNo(), reservedPartitions));

        repartitionedRDD.count();
//
//        Map<Integer, Map<String, Integer>> distribution = repartitionedRDD.mapPartitionsToPair(it -> {
//            Map<String, Integer> f = new HashMap<>();
//            it.forEachRemaining(t -> {
//                Integer previous = f.get(t._1);
//                if (previous == null) {
//                    f.put(t._1, 1);
//                } else {
//                    f.put(t._1, previous + 1);
//                }
//            });
//            return Collections.singletonList(new Tuple2<>(TaskContext.getPartitionId(), f)).iterator();
//        }).collectAsMap();
//
//        Map<Integer, Integer> info = new HashMap<>();
//        distribution.forEach((integer, map) -> info.put(integer, map.size()));
//        System.out.println(info.toString());
//
//
//        //System.out.println(repartitionedRDD.count());
//        t2 = System.currentTimeMillis();
//        System.out.println(t2 - t1);
        System.in.read();

    }


}
