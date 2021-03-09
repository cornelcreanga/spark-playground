package com.creanga.playground.spark.example.deduplication

import java.util.UUID

import com.creanga.playground.spark.util.RandomRDD
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._


object DeduplicationExample {


  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder()
        .appName("Streaming events").master("local[2]")
        .getOrCreate()
    val sc = sparkSession.sparkContext

    var t1 = 0L
    var t2 = 0L

    val rdd = new RandomRDD[Item](sc,
      sc.defaultParallelism,
      sc.defaultParallelism * 1000000, // will crash without enough memory (12-16gb)
      context = Map("duplicationProbabilityPercent" -> "0.1", "duplicationItems" -> "10"),
      generatorFunction = generateRandomItem
    )

    rdd.persist(StorageLevel.MEMORY_ONLY)
    println(s"count=${rdd.count()}")

    val df = sparkSession.createDataFrame(rdd)
    df.printSchema()

//    about 2x faster than the second option
//    val byBucket = Window.partitionBy("id").orderBy(col("timestamp").desc)
//    t1 = System.currentTimeMillis()
//    val deduplicatedDF = df.withColumn("rank", rank.over(byBucket)).where("rank = 1")
//    println(deduplicatedDF.count())
//    t2 = System.currentTimeMillis()
//    println(t2 - t1)


    import sparkSession.implicits._
    val deduplicatedDF = df.as[Item]
        .groupByKey(_.id)
        .reduceGroups((x, y) => if (x.timestamp > y.timestamp) x else y)
        .map(_._2)

    t1 = System.currentTimeMillis()
    println(deduplicatedDF.count())
    t2 = System.currentTimeMillis()
    println(t2 - t1)

    Thread.sleep(10*60*1000)

  }


  def generateRandomItem(context: Map[String, String]): Seq[Item] = {
    val duplicate = (1-context("duplicationProbabilityPercent").toDouble) < Math.random()
    if (duplicate) {
      val items = (context("duplicationItems").toInt * Math.random()).toInt + 1
      val id = UUID.randomUUID().toString
      (0 until items).view.map(counter => {
        Item(id, UUID.randomUUID().toString)
      })
    } else {
      Array(Item(UUID.randomUUID().toString, UUID.randomUUID().toString))
    }

  }
}
