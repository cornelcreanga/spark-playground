package com.creanga.playground.spark.csv;

import org.apache.commons.validator.routines.DomainValidator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.api.java.function.FlatMapGroupsFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.StructType;

import java.util.Collections;
import java.util.Iterator;

import static org.apache.spark.sql.types.DataTypes.*;

public class Ml {

    private static final Logger LOG = LogManager.getLogger(Ml.class);


    private static UDF1<String, Boolean> isValidUrl() {
        return (s1) -> DomainValidator.getInstance().isValid(s1);
    }

    private static UDF1<String, String> normalizePhone() {
        //todo - use PhoneNumberUtil phoneUtil = PhoneNumberUtil.getInstance();
        //todo - this can be optimized
        return (s1) -> {
            if (s1 == null)
                return null;
            if (s1.contains("E"))
                return null;
            return s1.replace("+", "");
        };
    }

    public static void main(String[] args) throws InterruptedException {
        SparkSession spark = SparkSession.builder()
                .master("local[16]")
                .appName("Reconciliation")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();

        spark.sqlContext().udf().register("isValidUrl", isValidUrl(), BooleanType);
        spark.sqlContext().udf().register("normalizePhone", normalizePhone(), StringType);

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

        fb = fb.filter("isValidUrl(domain)").filter(fb.col("name").isNotNull());
        fb.persist();
        google = google.filter("isValidUrl(domain)").filter(google.col("name").isNotNull());
        google.persist();
        web=web.filter("isValidUrl(root_domain)").filter(web.col("legal_name").isNotNull());
        web.persist();

        fb.createOrReplaceTempView("fb_raw");
        google.createOrReplaceTempView("google_raw");
        web.createOrReplaceTempView("web_raw");

        fb = spark.sql("select uuid() as id, lower(domain) as domain, lower(address) as address, lower(categories) as categories, lower(city) as city, lower(country_name) as country, lower(region_name) as region, normalizePhone(phone) as phone, lower(name) as name from fb_raw");
        google = spark.sql("select uuid() as id,lower(domain) as domain, lower(address) as address, lower(category) as categories, lower(city) as city, lower(country_name) as country, lower(region_name) as region, normalizePhone(phone) as phone, lower(name) as name  from google_raw");
        web = spark.sql("select uuid() as id,lower(root_domain) as domain, lower(s_category) as categories, lower(main_city) as city, lower(main_country) as country, lower(main_region) as region, normalizePhone(phone) as phone, lower(legal_name) as legal_name, lower(site_name) as site_name from web_raw");

        fb.createOrReplaceTempView("fb");
        google.createOrReplaceTempView("google");
        web.createOrReplaceTempView("web");
        LOG.info("FB length "+fb.count());
        LOG.info("Google length "+google.count());
        LOG.info("Web " + web.count());

        Dataset<Row> first = spark.sql(
                "select web.domain as webDomain," +
                        "fb.domain as fbDomain, " +
                        "fb.name as fbName, " +
                        "fb.phone as fbPhone, " +
                        "fb.city as fbCity, " +
                        "fb.country as fbCountry, " +
                        "fb.region as fbRegion, " +
                        "fb.categories as fbCategories, " +
                        "web.legal_name, " +
                        "web.site_name, " +
                        "web.phone as webPhone," +
                        "web.city as webCity, " +
                        "web.country as webCountry, " +
                        "web.region as webRegion, " +
                        "web.categories as webCategories " +
                        " from fb full outer join web on fb.domain=web.domain");
        first.createOrReplaceTempView("first");
//        first.show(100, false);
        first = processFirstDataset(first);
//        first.show(100, false);
        System.out.println("first:"+first.count());

        //split google into aggregators and non aggregators
        //fuzzy join first with aggregators
        //non aggregators - group them by domain and obtain another set (aka second).
        //fuzzy join second with aggregators

        Dataset<Row> facebookPages = spark.sql("select * from google where domain='facebook.com' and name!='facebook'");
        Dataset<Row> linkedinPages = spark.sql("select * from google where domain='linkedin.com' and name!='facebook'");
        Dataset<Row> youtubePages = spark.sql("select * from google where domain='youtube.com' and name!='facebook'");
        Dataset<Row> twitterPages = spark.sql("select * from google where domain='twitter.com' and name!='facebook'");
        Dataset<Row> instagramPages = spark.sql("select * from google where domain='instagram.com' and name!='instagram'");
        System.out.println("facebookPages:"+facebookPages.count());
        System.out.println("linkedinPages:"+linkedinPages.count());
        System.out.println("youtubePages:"+youtubePages.count());
        System.out.println("twitterPages:"+twitterPages.count());
        System.out.println("instagramPages:"+instagramPages.count());

//        google.groupByKey(new MapFunction<Row, Object>() {
//            @Override
//            public Object call(Row value) throws Exception {
//                return null;
//            }
//        }, Encoders.bean(Row.class));

        long c = google.groupByKey((MapFunction<Row, String>) row -> row.getAs("domain"), Encoders.STRING()).flatMapGroups(new FlatMapGroupsFunction<String, Row, Row>() {
            @Override
            public Iterator<Row> call(String key, Iterator<Row> values) throws Exception {
                while (values.hasNext()) {
                    Row next = values.next();
                    System.out.println(next.getAs("domain") + " - " + next.getAs("name"));
                }
                return Collections.singleton(Row.empty()).iterator();
            }
        }, Encoders.bean(Row.class)).count();

        System.out.println("C="+c);

//        FuzzyMatchPipeline<Row> fbPipeline = new FuzzyMatchPipeline<>(first, facebookPages);
//        FuzzyMatchPipeline<Row> linkedinPipeline = new FuzzyMatchPipeline<>(first, linkedinPages);
//        FuzzyMatchPipeline<Row> youtubePipeline = new FuzzyMatchPipeline<>(first, youtubePages);
//        FuzzyMatchPipeline<Row> twitterPipeline = new FuzzyMatchPipeline<>(first, twitterPages);
//        FuzzyMatchPipeline<Row> instagramPipeline = new FuzzyMatchPipeline<>(first, instagramPages);
//        Dataset<Row> firstFb = fbPipeline.join();
//        Dataset<Row> firstLinkedin = linkedinPipeline.join();
//        Dataset<Row> firstYoutube = youtubePipeline.join();
//        Dataset<Row> firstTwitter = twitterPipeline.join();
//        Dataset<Row> firstInstagram = instagramPipeline.join();
//        System.out.println("firstFb:"+firstFb.count());
//        System.out.println("firstLinkedin:"+firstLinkedin.count());
//        System.out.println("firstYoutube:"+firstYoutube.count());
//        System.out.println("firstTwitter:"+firstTwitter.count());
//        System.out.println("firstInstagram:"+firstInstagram.count());



//        Dataset<Row> agg = spark.sql("select domain from (select domain, count(domain) as no from google group by domain having no>50 order by no desc)");
//
//        agg.createOrReplaceTempView("aggregator");
//        System.out.println(spark.sql("select * from aggregator").count());
//
//        System.out.println(spark.sql("select * from google join aggregator on google.domain=aggregator.domain").count());





//        KeyValueGroupedDataset<String, Row> keyValues = google.groupByKey((MapFunction<Row, String>) row -> row.getAs("domain"), Encoders.STRING());
//        keyValues.flatMapGroups(new FlatMapGroupsFunction<String, Row, Row>() {
//            @Override
//            public Iterator<Row> call(String key, Iterator<Row> values) throws Exception {
//                while (values.hasNext()) {
//                    Row next = values.next();
//                    //Row transformed = RowFactory.create(next.)
//
//                }
//                return null;
//            }
//        }, Encoders.bean(Row.class));




        /**
         from pyspark.ml import Pipeline
         from pyspark.ml.feature import StopWordsRemover, Tokenizer, NGram, HashingTF, MinHashLSH, RegexTokenizer, SQLTransformer
         model = Pipeline(stages=[
         SQLTransformer(statement="SELECT *, lower(Title) lower FROM __THIS__"),
         Tokenizer(inputCol="lower", outputCol="token"),
         StopWordsRemover(inputCol="token", outputCol="stop"),
         SQLTransformer(statement="SELECT *, concat_ws(' ', stop) concat FROM __THIS__"),
         RegexTokenizer(pattern="", inputCol="concat", outputCol="char", minTokenLength=1),
         NGram(n=2, inputCol="char", outputCol="ngram"),
         HashingTF(inputCol="ngram", outputCol="vector"),
         MinHashLSH(inputCol="vector", outputCol="lsh", numHashTables=3)
         ]).fit(lens_ddf)
         result_lens = model.transform(lens_ddf)
         result_lens = result_lens.filter(F.size(F.col("ngram")) > 0)
         */


        ;


//        Dataset<Row> union = fb.union(google).union(web);
//
//        Dataset<Row> unionDomain = union.filter(union.col("domain").equalTo("facebook.com"));
//        unionDomain.orderBy(functions.col("name").desc()).show(200, false);


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

    private static Dataset<Row> processFirstDataset(Dataset<Row> first) {

        StructType schema = new StructType()
                .add("name", StringType)
                .add("domain", StringType)
                .add("categories", StringType)
                .add("city", StringType)
                .add("country", StringType)
                .add("region", StringType)
                .add("phone", StringType);

        Encoder rw = RowEncoder.encoderFor(schema);

        return first.map((MapFunction<Row, Row>) value -> {


            String domain = value.getAs("webDomain");
            String name = null;
            String categories = null;
            String city = null;
            String country = null;
            String region = null;
            String phone = null;
            if (domain==null){//only fb
                domain = value.getAs("fbDomain");
                name = value.getAs("fbName");
                categories = value.getAs("fbCategories");
                city = value.getAs("fbCity");
                country = value.getAs("fbCountry");
                region = value.getAs("fbRegion");
                phone = value.getAs("fbPhone");
            }else{
                if (value.getAs("fbDomain")==null){
                    domain = value.getAs("webDomain");
                    name = value.getAs("site_name");
                    categories = value.getAs("webCategories");
                    city = value.getAs("webCity");
                    country = value.getAs("webCountry");
                    region = value.getAs("webRegion");
                    phone = value.getAs("webPhone");
                }else{
                    domain = value.getAs("fbDomain");
                    name = concat(value.getAs("fbName"), value.getAs("site_name"));
                    categories = concat(value.getAs("fbCategories"),value.getAs("webCategories"));
                    city = concat(value.getAs("fbCity"),value.getAs("webCity"));
                    country = concat(value.getAs("fbCountry"),value.getAs("webCountry"));
                    region = concat(value.getAs("fbRegion"),value.getAs("webRegion"));
                    phone = concat(value.getAs("fbPhone"),value.getAs("webPhone"));
                }
            }
            GenericRowWithSchema row = new GenericRowWithSchema(new String[]{name,domain, categories, city, country, region, phone}, schema);
//            Company company = new Company();
//            company.setName(name);
//            company.setDomain(domain);
//            company.setCategories(categories);
//            company.setCity(city);
//            company.setCountry(country);
//            company.setRegion(region);
//            company.setPhone(phone);

            return row;

        },rw);
    }

    private static String concat(String s1, String s2){
        if (s1==null) {
            return s2;
        }
        if (s2==null) {
            return s1;
        }
//        if (s1.contains("|")) - todo
        if (s1.equals(s2)){
            return s1;
        }
        return s1 + " | " + s2;
    }
}

