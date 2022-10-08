package com.creanga.playground.spark.example.custompartitioner;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.HashPartitioner;
import org.apache.spark.RangePartitioner;
import org.apache.spark.SparkContext;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import scala.math.Ordering;
import scala.math.Ordering$;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.*;

public class RangeVsHashPartitioner {

    public static List<Tuple2<String, String>> generateList() {

        List<Tuple2<String, String>> result = new ArrayList<>();
        String uuid = UUID.randomUUID().toString();
        for (int i = 0; i < 600000; i++) {
            result.add(new Tuple2<>(uuid, RandomStringUtils.random(128, true, false)));
        }
        uuid = UUID.randomUUID().toString();
        for (int i = 0; i < 200000; i++) {
            result.add(new Tuple2<>(uuid, RandomStringUtils.random(128, true, false)));
        }
        for (int i = 0; i < 100; i++) {
            uuid = UUID.randomUUID().toString();
            for (int j = 0; j < (200000 * Math.random()); j++) {
                result.add(new Tuple2<>(uuid, RandomStringUtils.random(128, true, false)));
            }

        }
        Collections.shuffle(result);
        return result;
    }

    public static void main(String[] args) throws IOException {
        SparkSession sparkSession = SparkSession.builder()
                .master("local[4]")
                .appName("Spark streaming")
                .getOrCreate();

        final int partitions = 32;
        final File pathRangePartitioner = new File("/tmp/range/txt/");
        final File pathHashPartitioner = new File("/tmp/hash/txt/");
        Random rand = new Random();
        final int skewFactor = 20;

        SparkContext sc = sparkSession.sparkContext();
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(sc);

        List<Tuple2<String, String>> list = generateList();
        System.out.println("list length:" + list.size());
        JavaPairRDD<String, String> rdd = jsc.parallelizePairs(list);

        JavaPairRDD<String, String> rddUnskewed = rdd.mapToPair(t -> new Tuple2<>(t._1 + "_" + rand.nextInt(skewFactor), t._2));

        Ordering<String> ordering = Ordering$.MODULE$.comparatorToOrdering(Comparator.<String>naturalOrder());
        ClassTag<String> classTag = ClassTag$.MODULE$.apply(String.class);
        RangePartitioner<String, String> partitioner = new RangePartitioner<>(partitions, rddUnskewed.rdd(), true, ordering, classTag);
        JavaPairRDD<String, String> rddRangePartitioned = rddUnskewed.partitionBy(partitioner);
        JavaPairRDD<String, String> rddHashPartitioned = rddUnskewed.partitionBy(new HashPartitioner(partitions));

        List<Tuple2<Integer, Long>> data = getElementsPerPartition(rddRangePartitioned);
        System.out.println(data);
        data = getElementsPerPartition(rddHashPartitioned);
        System.out.println(data);


        JavaPairRDD<String, String> rddRangePartitionedToProcess =
                rddRangePartitioned.mapToPair(t -> new Tuple2<>(StringUtils.substringBefore(t._1, "_"), t._2));
        JavaPairRDD<String, String> rddHashPartitionedToProcess =
                rddHashPartitioned.mapToPair(t -> new Tuple2<>(StringUtils.substringBefore(t._1, "_"), t._2));

        FileUtils.deleteDirectory(pathRangePartitioner);
        FileUtils.deleteDirectory(pathHashPartitioner);
        FileUtils.forceMkdir(pathRangePartitioner);
        FileUtils.forceMkdir(pathHashPartitioner);

        processData(pathRangePartitioner, rddRangePartitionedToProcess);
        processData(pathHashPartitioner, rddHashPartitionedToProcess);

        System.out.println(FileUtils.listFiles(pathRangePartitioner, null, false).size());
        System.out.println(FileUtils.listFiles(pathHashPartitioner, null, false).size());

        sc.stop();

    }

    private static List<Tuple2<Integer, Long>> getElementsPerPartition(JavaPairRDD<String, String> rddRangePartitioned) {
        return rddRangePartitioned.mapPartitions((FlatMapFunction<Iterator<Tuple2<String, String>>, Tuple2<Integer, Long>>) it -> {
            long size = 0;
            while (it.hasNext()) {
                it.next();
                size++;
            }
            return Collections.singleton(new Tuple2<>(TaskContext.getPartitionId(), size)).iterator();
        }).collect();
    }

    private static void processData(File pathRangePartitioner, JavaPairRDD<String, String> rddRangePartitionedToProcess) {
        rddRangePartitionedToProcess.foreachPartition(it -> {
            Map<String, BufferedOutputStream> openedFiles = new HashMap<>();
            while (it.hasNext()) {
                Tuple2<String, String> next = it.next();
                String key = next._1;
                String value = next._2;
                int part = TaskContext.getPartitionId();
                BufferedOutputStream out = openedFiles.get(key);
                if (out == null) {
                    File file = new File(pathRangePartitioner.getPath() + "/" + key + "_" + part);
                    out = new BufferedOutputStream(new FileOutputStream(file));
                    openedFiles.put(key, out);
                }
                out.write(value.getBytes());
                out.write(13);
            }
            openedFiles.forEach((s, bufferedOutputStream) -> {
                try {
                    bufferedOutputStream.close();
                } catch (IOException e) {
                } //todo
            });
        });
    }

}
