package com.creanga.playground.spark.example.seqfiles;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LoadSequenceFiles {

    public static void main(String[] args) throws IOException {
        SparkSession sparkSession = SparkSession.builder()
                .master("local[8]")
                .appName("LoadSequenceFiles")
                .getOrCreate();

        File file = new File("/Users/cornel/seqfiles/");
        if (!file.exists()) {
            throw new RuntimeException("can't find " + file.getPath());
        }

        File[] directories = new File("/Users/cornel/seqfiles/").listFiles(File::isDirectory);
        if (directories == null) {
            throw new RuntimeException("empty folder " + file.getPath());
        }
        StringBuilder sb = new StringBuilder();
        for (File directory : directories) {
            sb.append("/Users/cornel/seqfiles/" + directory.getName()).append(",");
        }
        sb.deleteCharAt(sb.length() - 1);
        JavaRDD<Tuple2<NullWritable, BytesWritable>> seqFiles = sparkSession.sparkContext().sequenceFile(
                sb.toString(),
                NullWritable.class,
                BytesWritable.class,
                1).toJavaRDD();
        System.out.println(seqFiles.count());
        List<Map<String, Long>> list = seqFiles.mapPartitions(it -> {
            Map<String, Long> freqs = new HashMap<>();
            while (it.hasNext()) {
                Tuple2<NullWritable, BytesWritable> next = it.next();
                ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(next._2().getBytes()));
                PageView[] pageViews = (PageView[]) in.readObject();
                for (PageView pageView : pageViews) {
                    Long counter = freqs.get(pageView.personId);
                    if (counter == null) {
                        freqs.put(pageView.personId, 1L);
                    } else {
                        freqs.put(pageView.personId, counter + 1);
                    }
                }
            }
            return Collections.singletonList(freqs).listIterator();
        }).collect();
        System.out.println(list.size());

    }
}
