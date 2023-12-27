package com.creanga.playground.spark.csv;

import org.apache.commons.validator.routines.DomainValidator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;

public class Analyze {


    private static UDF1<String, Boolean> isValidUrl() {
        return (s1) -> DomainValidator.getInstance().isValid(s1);
    }

    public static void main(String[] args) throws InterruptedException {
        SparkSession spark = SparkSession.builder()
                .master("local[4]")
                .appName("Reconciliation")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();

        spark
                .sqlContext()
                .udf()
                .register("isValidUrl", isValidUrl(), DataTypes.BooleanType);

        Dataset<Row> fb = spark.read().
                format("csv").
                option("header", "true").
                load("/home/cornel/datasets/facebook_dataset.csv");
        Dataset<Row> google = spark.read().
                format("csv").
                option("header", "true").
                load("/home/cornel/datasets/google_dataset.csv");
        Dataset<Row> web = spark.read().
                format("csv").
                option("header", "true").
                option("delimiter", ";").
                load("/home/cornel/datasets/website_dataset.csv");


//        fb.persist();
//        google.persist();
//        web.persist();
        fb.createOrReplaceTempView("fb");
        google.createOrReplaceTempView("google");
        web.createOrReplaceTempView("web");

//
        spark.sql("select domain from fb order by domain asc").show(10000, false);
//        spark.sql("select count(*) from fb order by domain asc").show(1);
//        spark.sql("select count(*) from fb order by domain asc").show(1);
//        System.out.println(fb.select("domain").count());
//        System.out.println(fb.select("domain").filter("isValidUrl(domain)").count());


        fb = fb.filter("isValidUrl(domain)").filter(fb.col("name").isNotNull());
        google = google.filter("isValidUrl(domain)").filter(google.col("name").isNotNull());
        web = web.filter("isValidUrl(root_domain)").filter(web.col("legal_name").isNotNull());

        fb = fb.select("domain", "address", "categories", "city", "country_name", "region_name", "phone", "name").
                withColumn("origin", functions.lit("fb")).
                withColumnRenamed("country_name", "country").
                withColumnRenamed("region_name", "region");
        google = google.
                select("domain", "address", "category", "city", "country_name", "region_name", "phone", "name").
                withColumnRenamed("category", "categories").
                withColumnRenamed("country_name", "country").
                withColumnRenamed("region_name", "region").
                withColumn("origin", functions.lit("google"));

        web = web.
                select("root_domain", "s_category", "main_country", "main_region", "phone", "legal_name").
                withColumnRenamed("root_domain", "domain").
                withColumnRenamed("main_country", "country").
                withColumnRenamed("main_region", "region").
                withColumnRenamed("s_category", "categories").
                withColumnRenamed("legal_name", "name").
                withColumn("address", functions.lit("")).
                withColumn("city", functions.lit("")).
                withColumn("origin", functions.lit("web")).
                select("domain", "address", "categories", "city", "country", "region", "phone", "name", "origin");


        Dataset<Row> union = fb.union(google).union(web);

        Dataset<Row> unionDomain = union.filter(union.col("domain").equalTo("facebook.com"));
        unionDomain.orderBy(functions.col("name").desc()).show(200, false);


//        Dataset<Row> siteFb = fb.select("domain").distinct();
//        Dataset<Row> siteGoogle = google.select("domain").distinct();
//        Dataset<Row> sites = web.select("root_domain").distinct();
//
//        System.out.println(siteFb.count());
//        System.out.println(siteGoogle.count());
//        System.out.println(sites.count());
//
//
//        //drop empty names
//        web.select("legal_name","site_name").show(100,false);
//        System.out.println(web.filter(web.col("site_name").isNotNull()).count());
//        System.out.println(web.filter(web.col("site_name").isNull()).count());
//
//
//        Dataset<Row> fbInvalid = fb.filter(fb.col("name").isNull());
//        Dataset<Row> googleInvalid = google.filter(google.col("name").isNull());
//        Dataset<Row> webInvalid = web.filter(web.col("legal_name").isNull());
//
//        System.out.println(fbInvalid.count());
//        System.out.println(googleInvalid.count());
//        System.out.println(webInvalid.count());
//
//        fb = fb.filter(fb.col("name").isNotNull());
//        google = google.filter(google.col("name").isNotNull());
//        web = web.filter(web.col("legal_name").isNotNull());
//
//
////        fb.show(10, false);
////        google.show(10, false);
//
////        fb.printSchema();
////        google.printSchema();
////        web.printSchema();
//
//        fb = fb.select("domain", "address", "categories", "city", "country_name", "region_name", "phone", "name").
//                withColumn("origin", functions.lit("fb")).
//                withColumnRenamed("country_name", "country").
//                withColumnRenamed("region_name", "region");
//        google = google.
//                select("domain", "address", "category", "city", "country_name", "region_name", "phone", "name").
//                withColumnRenamed("category", "categories").
//                withColumnRenamed("country_name", "country").
//                withColumnRenamed("region_name", "region").
//                withColumn("origin",functions.lit("google"));
//
//        web = web.
//                select("root_domain", "s_category", "main_country", "main_region", "phone", "legal_name").
//                withColumnRenamed("root_domain", "domain").
//                withColumnRenamed("main_country", "country").
//                withColumnRenamed("main_region", "region").
//                withColumnRenamed("s_category", "categories").
//                withColumnRenamed("legal_name", "name").
//                withColumn("address", functions.lit("")).
//                withColumn("city", functions.lit("")).
//                withColumn("origin",functions.lit("web")).
//                select("domain", "address", "categories", "city", "country", "region", "phone", "name","origin");
//
//        Dataset<Row> fbFiltered = fb.filter(fb.col("domain").equalTo("1computerservices.com"));
//        Dataset<Row> googleFiltered = google.filter(google.col("domain").equalTo("1computerservices.com"));
//        Dataset<Row> webFiltered = web.filter(web.col("domain").equalTo("1computerservices.com"));
//
////        fb = fb.orderBy(functions.col("domain").desc());
////        google.groupBy("domain").count().orderBy(functions.col("count").desc(),functions.col("domain").asc()).show(10, false);
////        Dataset<Row> googleFb = google.filter(google.col("domain").equalTo("facebook.com")).filter(google.col("phone").isNull());
//
////        google.groupBy("name").count().orderBy(functions.col("count").desc(),functions.col("name").asc()).show(20, false);
//
////        google.groupBy("name").count().filter(functions.col("count").equalTo(2)).orderBy(functions.col("count").desc(),functions.col("name").asc()).show(200, false);
//
////        google.filter(google.col("name").equalTo("Angel Nails")).show(20, false);
//        //Lincoln Elementary School
//
//
////        googleFb.show(10, false);
////        fbFiltered.printSchema();
////        googleFiltered.printSchema();
////        webFiltered.printSchema();
//
//        fbFiltered.show(10, false);
//        googleFiltered.show(10, false);
//        webFiltered.show(10, false);
//
//        Dataset<Row> union = fb.union(google).union(web);
//
//        Dataset<Row> unionDomain = google.filter(google.col("domain").equalTo("facebook.com"));
//        unionDomain.groupBy("name").count().orderBy(functions.col("count").desc(),functions.col("name").asc()).show(200, false);
//
//        Dataset<CompanyInfo> ds = fb.union(google).union(web).as(Encoders.bean(CompanyInfo.class));
//
//        union.groupBy("domain").count().orderBy(functions.col("count").desc(),functions.col("domain").asc()).show(20, false);
//
//
//
//        //ds.show(10);
//
//
//        ds.groupByKey(new MapFunction<CompanyInfo, String>() {
//            @Override
//            public String call(CompanyInfo companyInfo) throws Exception {
//                return companyInfo.getDomain();
//            }
//        },Encoders.STRING()).reduceGroups(new ReduceFunction<CompanyInfo>() {
//            @Override
//            public CompanyInfo call(CompanyInfo c1, CompanyInfo c2) throws Exception {
//                return c1;
//            }
//        }).show(10);
//
////        final Dataset<CompanyInfo> dataset;
////        sparkSession.createDataset(fb.union(google).union(web).rdd(), bean);
//
////        union.groupByKey((MapFunction<Row, String>) row -> row.getAs("domain"), Encoders.STRING()).reduceGroups(new ReduceFunction<Row>() {
////            @Override
////            public Row call(Row row, Row t1) throws Exception {
////                return null;
////            }
////        });
//
////        System.out.println(union.count());
////        union.show(1000, false);
//
////        web.sort(web.col("root_domain")).show(10000, false);
//
//
////        Dataset<Row> join = fb.join(google, fb.col("domain").equalTo(google.col("domain")), "inner");
////        System.out.println(join.count());
////        join.show(10, false);
        Thread.sleep(600000);
    }

}
