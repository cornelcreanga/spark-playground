package com.creanga.playground.spark.example.custompartitioner;

import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;
import scala.runtime.AbstractFunction1;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class ExampleJava {

    public static class JobFunc<T, U> extends AbstractFunction1<Iterator<T>, Boolean> implements Serializable {


        @Override
        public Boolean apply(Iterator<T> iterator) {
            return true;
        }
    }

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder()
                .master("local[3]")
                .appName("Spark streaming")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();

        ClassTag<Integer> uClassTag = ClassTag$.MODULE$.apply(Integer.class);
        JavaSparkContext sc = new JavaSparkContext(sparkSession.sparkContext());
        ArrayList<String> paths = new ArrayList<>();

        ArrayList<LocationMetadata> metadata = new ArrayList<>();
        metadata.add(new LocationMetadata(new Location("1", "10", 100), new String[]{"a", "b"}));
        metadata.add(new LocationMetadata(new Location("1", "11", 100), new String[]{"a", "b"}));
        metadata.add(new LocationMetadata(new Location("1", "12", 100), new String[]{"a", "b"}));
        metadata.add(new LocationMetadata(new Location("1", "13", 100), new String[]{"a", "b"}));
        metadata.add(new LocationMetadata(new Location("1", "14", 100), new String[]{"a", "b"}));
        metadata.add(new LocationMetadata(new Location("1", "15", 100), new String[]{"a", "b"}));
        metadata.add(new LocationMetadata(new Location("1", "16", 100), new String[]{"a", "b"}));
        metadata.add(new LocationMetadata(new Location("1", "17", 100), new String[]{"a", "b"}));
        metadata.add(new LocationMetadata(new Location("1", "18", 100), new String[]{"a", "b"}));
        metadata.add(new LocationMetadata(new Location("1", "19", 100), new String[]{"a", "b"}));
        metadata.add(new LocationMetadata(new Location("1", "20", 100), new String[]{"a", "b"}));
        metadata.add(new LocationMetadata(new Location("1", "30", 100), new String[]{"a", "b"}));
        metadata.add(new LocationMetadata(new Location("1", "40", 100), new String[]{"a", "b"}));
        metadata.add(new LocationMetadata(new Location("1", "50", 100), new String[]{"a", "b"}));
        metadata.add(new LocationMetadata(new Location("1", "60", 100), new String[]{"a", "b"}));
        metadata.add(new LocationMetadata(new Location("1", "70", 100), new String[]{"a", "b"}));
        metadata.add(new LocationMetadata(new Location("1", "80", 100), new String[]{"a", "b"}));
        metadata.add(new LocationMetadata(new Location("1", "90", 800), new String[]{"a", "b"}));
        metadata.add(new LocationMetadata(new Location("1", "100", 500), new String[]{"a", "b"}));
        metadata.add(new LocationMetadata(new Location("1", "110", 700), new String[]{"a", "b"}));
        metadata.add(new LocationMetadata(new Location("1", "120", 400), new String[]{"a", "b"}));
        metadata.add(new LocationMetadata(new Location("1", "130", 1400), new String[]{"a", "b"}));
        metadata.add(new LocationMetadata(new Location("1", "140", 200), new String[]{"a", "b"}));
        metadata.add(new LocationMetadata(new Location("1", "150", 500), new String[]{"a", "b"}));
        metadata.add(new LocationMetadata(new Location("1", "160", 2500), new String[]{"a", "b"}));
        metadata.add(new LocationMetadata(new Location("1", "170", 2500), new String[]{"a", "b"}));
        metadata.add(new LocationMetadata(new Location("1", "180", 2500), new String[]{"a", "b"}));


        RDD<String> rdd = sc.parallelize(paths).rdd();


        JavaRDD<LocationMetadata> metadataRdd = sc.parallelize(metadata);


        long totalSize = metadataRdd.
                map((Function<LocationMetadata, Long>) v1 -> v1.getLocation().getFilesTotalSize()).
                reduce((Function2<Long, Long, Long>) (a, b) -> Long.sum(a, b));
        long partitionSize = totalSize / sc.defaultParallelism();

        System.out.println(totalSize + "-" + partitionSize);

        JavaRDD<List<LocationMetadata>> metadataRddList = metadataRdd.glom().flatMap(locationMetadata -> {
            ArrayList<List<LocationMetadata>> list = new ArrayList<>();
            List<LocationMetadata> sublist = new ArrayList<>();
            long size = 0;
            for (int i = 0; i < locationMetadata.size(); i++) {
                LocationMetadata item = locationMetadata.get(i);
                size += item.getLocation().getFilesTotalSize();
                if (size < partitionSize) {
                    sublist.add(item);
                } else {
                    list.add(new ArrayList<>(sublist));
                    size = 0;
                    sublist = new ArrayList<>();
                    sublist.add(item);
                }
            }
            if (sublist.size() > 0)
                list.add(new ArrayList<>(sublist));
            return list.iterator();
        });

        List<List<LocationMetadata>> collected = metadataRddList.collect();
        collected.forEach(System.out::println);
        System.out.println(metadataRddList.count());

        metadataRddList.foreachPartition((VoidFunction<Iterator<List<LocationMetadata>>>) listIterator ->
                listIterator.forEachRemaining(locationMetadata ->
                        locationMetadata.forEach(locationMetadata1 ->
                                System.out.println(TaskContext.getPartitionId() + "-" + locationMetadata1.getLocation()))));

        //metadataRddList.repartition(sc.defaultParallelism());
        System.out.println("--------------------------------------------");
        metadataRddList.foreachPartition((VoidFunction<Iterator<List<LocationMetadata>>>) listIterator -> {
            int c = 0;
            while (listIterator.hasNext()) {
                c++;
                listIterator.next();
            }
            System.out.println(TaskContext.getPartitionId() + "-" + c);
        });

        System.out.println("--------------------------------------------");
        System.out.println("count:" + metadataRddList.count());
        JavaRDD<List<LocationMetadata>> repartitioned = metadataRddList.repartition((int) metadataRddList.count());
        repartitioned.foreachPartition((VoidFunction<Iterator<List<LocationMetadata>>>) listIterator ->
                listIterator.forEachRemaining(locationMetadata ->
                        locationMetadata.forEach(locationMetadata1 ->
                                System.out.println(TaskContext.getPartitionId() + "-" + locationMetadata1.getLocation()))));


        JavaPairRDD<Location, String[]> pairdRdd = metadataRdd.mapToPair(
                (PairFunction<LocationMetadata, Location, String[]>) locationMetadata ->
                        new Tuple2<>(locationMetadata.getLocation(), locationMetadata.getFiles()
                        )
        );


        Integer[] taskResult = (Integer[]) pairdRdd.rdd().context().runJob(pairdRdd.rdd(),
                new AbstractSerializableFunction2<TaskContext, scala.collection.Iterator<Tuple2<Location, String[]>>, Integer>() {
                    @Override
                    public Integer apply(TaskContext context, scala.collection.Iterator<Tuple2<Location, String[]>> rows) {
                        rows.foreach(new AbstractFunction1<Tuple2<Location, String[]>, Object>() {

                            @Override
                            public Object apply(Tuple2<Location, String[]> v1) {
                                // System.out.println(TaskContext.getPartitionId() + "-" + v1._1);
                                return null;
                            }
                        });
//                        rows.foreach(row->{
//                            System.out.println(row._1);
//                        });
                        return 1;
                    }
                }, uClassTag
        );

        System.out.println(pairdRdd.partitions().size());


    }
}
