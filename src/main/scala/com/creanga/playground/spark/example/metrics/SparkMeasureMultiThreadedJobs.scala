package com.creanga.playground.spark.example.metrics

import java.util.concurrent.TimeUnit

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

import org.apache.spark.scheduler.{SparkListener, SparkListenerStageSubmitted, SparkListenerTaskEnd}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object SparkMeasureMultiThreadedJobs {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
        .setMaster("local[4]")
        .setAppName("StreamingTransactionExample")
        .set("spark.scheduler.mode", "FAIR")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().getOrCreate()

    val first = Future({
      sc.setLocalProperty("spark.scheduler.pool", "fair")
      sc.setLocalProperty("job-context", "first")
      val listener = new MetricListener("first")
      sc.addSparkListener(listener)
      val df = spark.read.load("/home/cornel/data.parquet")

      val countResult = df.filter(df("_verizon.emsLessDns") === "Y").count()
      println(countResult)
      println(listener.rows)
      sc.removeSparkListener(listener)
    })

    val second = Future({
      sc.setLocalProperty("spark.scheduler.pool", "fair")
      sc.setLocalProperty("job-context", "second")
      val listener = new MetricListener("second")
      sc.addSparkListener(listener)
      val df = spark.read.load("/home/cornel/data.parquet")

      val countResult = df.filter(df("_verizon.emsLessDns") === "Y").count()
      println(countResult)
      println(listener.rows)
      sc.removeSparkListener(listener)
    })

    Await.result(first, Duration(60, TimeUnit.SECONDS))
    Await.result(second, Duration(60, TimeUnit.SECONDS))

  }

}
