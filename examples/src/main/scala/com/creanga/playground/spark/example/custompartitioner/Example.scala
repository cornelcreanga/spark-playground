package com.creanga.playground.spark.example.custompartitioner

import com.creanga.playground.spark.util.RandomRDD
import org.apache.spark.sql.SparkSession

import java.time.{Month, ZoneOffset}
import java.util.UUID
import java.util.concurrent.ThreadLocalRandom

object Example {

  import java.time.LocalDate

  def between(startInclusive: LocalDate, endExclusive: LocalDate): LocalDate = {
    val startEpochDay = startInclusive.toEpochDay
    val endEpochDay = endExclusive.toEpochDay
    val randomDay = ThreadLocalRandom.current.nextLong(startEpochDay, endEpochDay)
    LocalDate.ofEpochDay(randomDay)
  }

  def generateRandomItem(context: Map[String, String]): Seq[Item] = {
    val start = LocalDate.of(2010, Month.OCTOBER, 14)
    val end = LocalDate.now
    Seq(Item(UUID.randomUUID().toString, "name", 10, between(start, end).atStartOfDay().toInstant(ZoneOffset.UTC).toEpochMilli))
  }

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("Partitioner").master("local[2]")
      .getOrCreate()
    val sc = sparkSession.sparkContext

    val randomRdd = new RandomRDD[Item](sc,
      sc.defaultParallelism,
      sc.defaultParallelism * 1000,
      context = Map.empty[String, String],
      generatorFunction = generateRandomItem
    )
    val processor = new Processor()
    val partitionedRdd = randomRdd.map(item => (item.date, item)).partitionBy(new MonthPartitioner)
    val jobResults = sc.runJob(partitionedRdd, processor.processAndWriteData _)
    val successfulResults = jobResults.filter(job=>job.isInstanceOf[ProcessorResultSuccessful])
    val failedResults = jobResults.filter(job=>job.isInstanceOf[ProcessorResultFailed])
    println(successfulResults.length)
    println(failedResults.length)

  }

}
