package com.creanga.playground.spark.csv;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;
import java.util.*;

import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.StringType;

public class TestMl {

    public static class Test implements Serializable {
        private String id;
        private String name;

        public Test(String id, String name) {
            this.id = id;
            this.name = name;
        }

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }
    }

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .master("local[16]")
                .appName("TestMl")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();

        SparkContext sc = spark.sparkContext();
        JavaSparkContext jsc = new JavaSparkContext(sc);

        List<Test> list1 = new ArrayList<>();
        list1.add(new Test("a1","Investa Wholesale Funds Management Limited"));
        list1.add(new Test("a2","Investa Wholesale Funds Management"));


        List<Test> list2 = new ArrayList<>();
        list2.add(new Test("b1","Investa Wholesale Funds "));
        list2.add(new Test("b2","Investa Wholesale Funds Limited"));
        list2.add(new Test("b3","Bu hu hu"));

        StructType schema = new StructType()
                .add("name", StringType)
                .add("id", StringType)
                .add("gender", StringType)
                .add("salary", IntegerType);

        Dataset<Test> ds1 = spark.createDataset(jsc.parallelize(list1).rdd(), Encoders.bean(Test.class));
        Dataset<Test> ds2 = spark.createDataset(jsc.parallelize(list2).rdd(), Encoders.bean(Test.class));
        FuzzyMatchPipeline<Test> fuzzyMatchPipeline = new FuzzyMatchPipeline<>(ds1, ds2);
        Dataset<Row> joined  = fuzzyMatchPipeline.join();
        joined.printSchema();

        // withColumn("origin", functions.lit("fb")).
        WindowSpec windowSpec = Window.partitionBy("datasetA.id");
//        joined = joined.withColumn("minDist", functions.min("jaccardDist").over(windowSpec)).where(functions.col("jaccardDist").equalTo(functions.col("minDist"))).drop("minDist");
        joined = joined.filter("datasetA.id!=datasetB.id");
        joined.select("datasetA.id","datasetB.id","datasetA.name","datasetB.name").show(20, false);


        joined.persist();




        System.out.println(joined.count());
        joined.show(10, false);

    }

}
