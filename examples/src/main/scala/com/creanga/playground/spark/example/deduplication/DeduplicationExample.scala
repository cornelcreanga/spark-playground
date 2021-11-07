package com.creanga.playground.spark.example.deduplication

import com.creanga.playground.spark.util.RandomRDD
import com.creanga.playground.spark.util.Utils.randomArray
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, rank}
import org.apache.spark.storage.StorageLevel

import java.util.UUID


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
      sc.defaultParallelism * 100000, // will crash without enough memory
      context = Map("duplicationProbabilityPercent" -> "0.1", "duplicationItems" -> "10", "bodyLength" -> "5000"),
      generatorFunction = generateRandomItem
    )

    rdd.persist(StorageLevel.MEMORY_ONLY)
    println(s"count=${rdd.count()}")

    val df = sparkSession.createDataFrame(rdd)
    df.printSchema()


    println("-------------------------------------------------------------------------------")
    var byBucket = Window.partitionBy("id").orderBy(col("timestamp").desc)
    t1 = System.currentTimeMillis()
    var deduplicatedDF = df.withColumn("rank", rank.over(byBucket)).where("rank = 1").drop("rank")
    println(deduplicatedDF.count())
    t2 = System.currentTimeMillis()
    println(t2 - t1)
    println("-------------------------------------------------------------------------------")

    //fastest version
    byBucket = Window.partitionBy("id")
    t1 = System.currentTimeMillis()
    deduplicatedDF = df.withColumn("max", functions.max("timestamp").over(byBucket)).where(col("timestamp") === col("max")).drop("max")
    println(deduplicatedDF.count())
    t2 = System.currentTimeMillis()
    println(t2 - t1)
    println("-------------------------------------------------------------------------------")

    import sparkSession.implicits._
    val deduplicatedDataset = df.as[Item]
      .groupByKey(_.id)
      .reduceGroups((x, y) => if (x.timestamp > y.timestamp) x else y)
      .map(_._2)

    t1 = System.currentTimeMillis()
    println(deduplicatedDataset.count())
    t2 = System.currentTimeMillis()
    println(t2 - t1)
    println("-------------------------------------------------------------------------------")

    import sparkSession.implicits._
    val reducedRdd = df.as[Item].rdd
      .groupBy(r => r.id)
      .map(item => {
        val list = item._2.toList
        (item._1, list.maxBy(_.timestamp(2)))
      }).map(_._2)
    deduplicatedDF = sparkSession.createDataFrame(reducedRdd)
    t1 = System.currentTimeMillis()
    println(deduplicatedDF.count())
    t2 = System.currentTimeMillis()
    println(t2 - t1)

    println("-------------------------------------------------------------------------------")

    sc.stop()
    //Thread.sleep(10 * 60 * 1000)

  }


  def generateRandomItem(context: Map[String, String]): Seq[Item] = {
    val bodyLength = context("bodyLength").toInt
    val duplicate = context("duplicationProbabilityPercent").toDouble > Math.random()
    if (duplicate) {
      val items = (context("duplicationItems").toInt * Math.random()).toInt + 1
      val id = UUID.randomUUID().toString
      (0 until items).view.map(counter => {
        Item(id, UUID.randomUUID().toString, randomArray(bodyLength))
      })
    } else {
      Array(Item(UUID.randomUUID().toString, UUID.randomUUID().toString, randomArray(bodyLength)))
    }

  }
}
