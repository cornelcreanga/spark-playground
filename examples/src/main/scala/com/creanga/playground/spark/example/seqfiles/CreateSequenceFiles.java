package com.creanga.playground.spark.example.seqfiles;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class CreateSequenceFiles {

    public static void main(String[] args) throws IOException {
        SparkSession sparkSession = SparkSession.builder()
                .master("local[8]")
                .appName("SeqFileGenerator")
                .getOrCreate();


        String[] personIds = new String[10];
        for (int i = 0; i < personIds.length; i++) {
            personIds[i] = UUID.randomUUID().toString();
        }
        int[] items = new int[]{
                1, 1, 1, 1, 1, 1, 4, 4, 8, 8
        };

        File file = new File("/Users/cornel/seqfiles/");
        if (file.exists()) {
            FileUtils.deleteDirectory(file);
        }

        for (int i = 0; i < personIds.length; i++) {
            String personId = personIds[i];
            int len = items[i] * 1_000_000;
            List<PageView> list = new ArrayList<>(len);
            String text = StringUtils.repeat("a", 100);
            for (int j = 0; j < len; j++) {
                list.add(new PageView(UUID.randomUUID().toString(), personId, System.currentTimeMillis(), text + RandomStringUtils.randomAlphabetic(28)));
            }
            Dataset<PageView> dataset = sparkSession.createDataset(list, Encoders.bean(PageView.class));
            dataset.rdd().saveAsObjectFile("/Users/cornel/seqfiles/" + personId);

//            JavaPairRDD<String, PageView> pairRdd = dataset.javaRDD().mapToPair(pageView -> new Tuple2<>(pageView.personId, pageView));
//            pairRdd.saveAsHadoopFile("/Users/cornel/seqfiles/" + personId, Text.class, BytesWritable.class, SequenceFileOutputFormat.class);

        }
    }
}
