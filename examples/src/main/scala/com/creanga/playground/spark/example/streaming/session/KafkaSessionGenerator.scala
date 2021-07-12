package com.creanga.playground.spark.example.streaming.session

import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.streaming.{GroupStateTimeout, OutputMode, Trigger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Dataset, SQLContext, SaveMode, SparkSession}

object KafkaSessionGenerator {

  def extractTripId(data: String): String = {
    val index1 = data.indexOf("tripEventId")
    if (index1 == -1)
      return null
    val index2 = data.indexOf("\"", index1 + 13)
    if (index2 == -1)
      return null
    val index3 = data.indexOf("\"", index2 + 1)
    if (index3 == -1)
      return null
    data.substring(index2 + 1, index3)
  }


  private val extractTripIdUDF = udf(extractTripId _)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
        .setMaster("local[2]")
        .setAppName("KafkaSessionProcessor")
    val sc = new SparkContext(conf)

    val spark = SparkSession.builder().getOrCreate()
    implicit val sqlContext: SQLContext = spark.sqlContext
    sqlContext.setConf("spark.sql.shuffle.partitions", "4")
    import spark.implicits._


    val inputStream = spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9093")
        .option("subscribe", "tripEvents,gpsEvents")
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
//        .option("minPartitions", "32")//spark>=2.4.5
        .load()

    val tripDf = inputStream.
//        repartition(8).
        selectExpr("CAST(value AS STRING)").
        withColumn("tripEventId", extractTripIdUDF(col("value"))).
        filter(col("tripEventId").isNotNull).
        groupByKey(row => row.getAs[String]("tripEventId"))
        .mapGroupsWithState(GroupStateTimeout.NoTimeout())(Mapping.buildSessions)

    val query = tripDf.writeStream
        .trigger(Trigger.ProcessingTime("60 seconds"))
        .option("checkpointLocation", s"/tmp/localfiles/checkpoint/")
        .outputMode("update")
        .foreachBatch { (batchDF: Dataset[SessionInfo2], batchId: Long) =>
          batchDF.
//              coalesce(1).
              write.
              format("json").
              mode(SaveMode.Overwrite).
              save(s"/tmp/localfiles/sessions/$batchId")
        }
        .start

    query.awaitTermination()

  }
}
