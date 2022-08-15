package com.creanga.playground.spark.example.custompartitioner;

import com.creanga.playground.spark.util.FastRandom;
import com.creanga.playground.spark.util.MapUtil;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.math3.distribution.EnumeratedDistribution;
import org.apache.commons.math3.util.Pair;
import org.apache.spark.SparkContext;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;
import scala.Tuple3;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.creanga.playground.spark.util.IOUtils.getResourceFileAsStream;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.counting;
import static java.util.stream.Collectors.groupingBy;

public class CustomPartitionerDemo {

    String path = "/tmp/data";

    public static List<Tuple2<String, Long>> generateList() throws IOException {
        List<String> lines = getResourceFileAsStream("stats.csv");
        List<Tuple2<String, Long>> list = new ArrayList<>();

        for (int i = 0; i < lines.size(); i++) {
            String[] items = lines.get(i).split("\t");
            list.add(new Tuple2<>(UUID.randomUUID().toString(), Long.parseLong(items[0]) * Long.parseLong(items[1])));
        }
        return list;
    }

    public static void main(String[] args) throws IOException {
        SparkSession sparkSession = SparkSession.builder()
                .master("local[8]")
                .appName("Spark streaming")
                .getOrCreate();
        SparkContext sc = sparkSession.sparkContext();
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(sc);
        int partitions = 16;
        int values = 10000000;

        long t1= System.currentTimeMillis();
        List<Tuple3<String, Integer, Integer>> stats = new ArrayList<>();
        try (InputStream in = CustomPartitionerDemo.class.getResourceAsStream("/stats.csv"); //cid, eventNo, eventsTotalsize, eventsTotalsize/eventNo
             BufferedReader reader = new BufferedReader(new InputStreamReader(in))) {
            String line ;
            while ((line = reader.readLine()) != null) {
                String[] items = line.split("\t");
                String cid = UUID.randomUUID().toString();
                stats.add(new Tuple3<>(cid,Integer.parseInt(items[0]),Integer.parseInt(items[1])));
            }
        } catch (Exception e) {
            throw new RuntimeException("cannot create the synthetic rdd, an error appeared during parsing the frequency file", e);
        }


        SyntheticRddProvider rddProvider = new SyntheticRddProvider(jsc,stats, partitions, values, new HashMap<>());
        JavaPairRDD<String, byte[]> pairRDD = rddProvider.buildKafkaRdd().mapToPair(stringTuple2 -> stringTuple2);

        pairRDD.persist(StorageLevel.MEMORY_ONLY());


       Map<String, Long> freqs = pairRDD.mapToPair( t -> new Tuple2<>(t._1, (long)t._2.length)).reduceByKeyLocally(Long::sum);


        long t2= System.currentTimeMillis();
        System.out.println(t2-t1);

//        System.out.println(freqs);

        t1= System.currentTimeMillis();
        long no = 0;

        //List<Tuple2<String, Long>> data = generateList();
        List<String> customersUUIDs = new ArrayList<>(freqs.keySet());
        List<Long> customers = new ArrayList<>(freqs.values());
        double[] customersPartitions = new double[customers.size()];
        for (long customer : customers) {
            no += customer;
        }

        long partitionCapacity = no / partitions;
        for (int i = 0; i < customers.size(); i++) {
            customersPartitions[i] = (double) customers.get(i) / partitionCapacity;
        }

//        System.out.printf("no=%s\n", no);
//        System.out.printf("partitionCapacity=%s\n", partitionCapacity);
//        for (int i = 0; i < customers.size(); i++) {
//            System.out.printf("customersPartitions[%s]=%,.2f\n", i, customersPartitions[i]);
//        }
        double[] partitionAvailability = new double[partitions];
        Arrays.fill(partitionAvailability, 1);
        Map<String, List<Tuple2<Integer,Double>>> distribution = new HashMap<>();
        for (int i = 0; i < customers.size(); i++) {
            long customer = customers.get(i);
            double customersPartition = customersPartitions[i];
            List<Tuple2<Integer,Double>> list = new ArrayList<>();
            for (int j = 0; j < partitionAvailability.length; j++) {
                if (customersPartition == 0)
                    break;

                double v = partitionAvailability[j];
                if (v == 0)
                    continue;
                if (customersPartition >= v) {
                    list.add(new Tuple2<>(j, v));
                    partitionAvailability[j] = 0;
                    customersPartition -= v;
                }else{
                    list.add(new Tuple2<>(j, customersPartition));
                    partitionAvailability[j] -= customersPartition;
                    customersPartition =0;

                }
            }
            distribution.put(customersUUIDs.get(i), list);
        }

        //Map<String, EnumeratedDistribution<Integer>> distributionMap = new HashMap<>();
        Map<String, Object> distributionMap = new HashMap<>();
        Set<String> keys = distribution.keySet();

        for (Iterator<String> iterator = keys.iterator(); iterator.hasNext(); ) {
            String next = iterator.next();
            List<Tuple2<Integer,Double>> list = distribution.get(next);
            if (list.size() == 1){
                distributionMap.put(next,list.get(0)._1);
            }else {
                List<Pair<Integer, Double>> pairs = list.stream().map(t -> new Pair<>(t._1, t._2)).collect(Collectors.toList());
                EnumeratedDistribution<Integer> ed = new EnumeratedDistribution<>(new FastRandom(), pairs);
                distributionMap.put(next, ed);
            }
        }

        t2= System.currentTimeMillis();
        System.out.println(t2-t1);
        // System.out.println(distribution);
        Broadcast<Map<String, Object>> distributionBroadcast = jsc.broadcast(distributionMap);

//        t1 = System.currentTimeMillis();
        JavaPairRDD<String, byte[]> repartitionedRDD = pairRDD.repartitionAndSortWithinPartitions(new CustomPartitioner(distributionBroadcast, partitions));
        //System.out.println(repartitionedRDD.count());
//        t2 = System.currentTimeMillis();
//        System.out.println(t2-t1);

        repartitionedRDD.mapPartitions(new FlatMapFunction<Iterator<Tuple2<String,byte[]>>, Object>() {
            @Override
            public Iterator<Object> call(Iterator<Tuple2<String, byte[]>> tuple2Iterator) throws Exception {
                return null;
            }
        });





    }

}
