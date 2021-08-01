package com.creanga.playground.spark.example.streaming.session

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

import com.creanga.playground.spark.util.Mapper.plainMapper
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.streaming.{GroupStateTimeout, OutputMode, Trigger}
import org.apache.spark.sql.{Dataset, SQLContext, SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

case class TickProducer(data: MemoryStream[String]) {
  private var prev = 0
  private var counter = 1

  def produce(second: Int): Unit = {
    Thread.sleep(Math.max((second - prev) * 1000, 0))
    val gps = GpsTick("1", System.currentTimeMillis(), counter.toString)
    data.addData(plainMapper.writeValueAsString(gps))
    counter = counter + 1
    prev = second
  }
}

case class TripProducer(data: MemoryStream[String]) {
  private var prev = 0

  def produce(second: Int, event: String, tripId: String): Unit = {
    Thread.sleep((second - prev) * 1000)
    val tripEvent = TripEvent("1", event, System.currentTimeMillis(), tripId)

    data.addData(plainMapper.writeValueAsString(tripEvent))
    prev = second
  }
}

object SessionProcessing {

  def extractDriverId(data: String): String = {
    val index1 = data.indexOf("driverId")
    if (index1 == -1)
      return null
    val index2 = data.indexOf("\"", index1 + 10)
    if (index2 == -1)
      return null
    val index3 = data.indexOf("\"", index2 + 1)
    if (index3 == -1)
      return null
    data.substring(index2 + 1, index3)
  }


  private val extractDriverIdUDF: UserDefinedFunction = udf(extractDriverId _)

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
        .setMaster("local[4]")
        .setAppName("SessionProcessing")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().getOrCreate()
    implicit val sqlContext: SQLContext = spark.sqlContext
    import spark.implicits._

    val data: MemoryStream[String] = MemoryStream[String]

    val producer = Future {
      val tickGps = TickProducer(data)
      val tickData = TripProducer(data)

      for (i <- Range(1, 10)) tickGps.produce(i)
      tickData.produce(10, "Start", "1")

      for (i <- Range(10, 70)) tickGps.produce(i)
      tickData.produce(70, "End", "1")

      for (i <- Range(70, 100)) tickGps.produce(i)
      tickData.produce(100, "Start", "2")

      for (i <- Range(100, 170)) tickGps.produce(i)
      tickData.produce(170, "End", "2")

      for (i <- Range(170, 200)) tickGps.produce(i)
    }

    val tripDf = data.toDF().
        withColumn("driverId", extractDriverIdUDF(col("value"))).
        filter(col("driverId").isNotNull).
        groupByKey(row => row.getAs[String]("driverId"))
        .flatMapGroupsWithState(OutputMode.Append, GroupStateTimeout.NoTimeout())(Mapping.filterNonSessionGpsEvents)

    val query = tripDf.writeStream
        .trigger(Trigger.ProcessingTime("10 seconds"))
        .foreachBatch { (batchDF: Dataset[GpsTick], batchId: Long) =>
          //batchDF.show(20, truncate = false)
          batchDF.coalesce(1).write.
              format("json").
              mode(SaveMode.Overwrite).
              save(s"/home/cornel/stream/$batchId")
        }
        .start


    query.awaitTermination()

  }

}
