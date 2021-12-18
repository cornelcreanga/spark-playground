package com.creanga.playground.spark.example.metrics

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import java.util.concurrent.TimeUnit
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

object SparkMeasureMultiThreadedJobs {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[8]")
      .setAppName("StreamingTransactionExample")
      .set("spark.scheduler.mode", "FAIR")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().getOrCreate()

    val futures = new Array[Future[Unit]](5)
    val stageMetrics = ch.cern.sparkmeasure.StageMetrics(spark)
    for (i <- 1 to 5) {
      futures(i - 1) = Future({
        sc.setLocalProperty("job-context", "" + i)
        sc.setLocalProperty("spark.jobGroup.id",""+i);

        val df = spark.read.load("/home/cornel/data" + i + ".parquet")
        val listener = new MetricListener("" + i)

        stageMetrics.begin()
        sc.addSparkListener(listener)
        val countResult = df.filter(df("_verizon.emsLessDns") === "Y").count()
        println(countResult)
        println(i + ": " + listener.rowsRead.get())
        sc.removeSparkListener(listener)

        stageMetrics.end()

      })
    }

    for (i <- 0 to 4) {
      Await.result(futures(i), Duration(600, TimeUnit.SECONDS))
    }
    stageMetrics.listenerStage
    println("done")
    val df = stageMetrics.createStageMetricsDF()
    stageMetrics.reportMap()
    df.printSchema();
    df.show(100)


  }

}
