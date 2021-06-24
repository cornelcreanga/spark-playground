package com.creanga.playground.spark.example.streaming

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

import com.creanga.playground.spark.example.streaming.TripEventType.TripEventType
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.functions.{col, explode, expr, to_timestamp}
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode, Trigger}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, SQLContext, SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}


//case class TripInterval(id:Long, var start:Long, var end:Long)
//case class TripEventState(driverId: String, trips:ArrayBuffer[TripInterval]) {
//}

case class TripSession(tripId:Long, var start:Long)
case class TripEventState(map: mutable.Map[String, TripSession])

object TripMapping {
  def mapStreamingLogsToState(driverId: String,
      tripEvents: Iterator[TripEvent],
      currentState: GroupState[TripEventState]): Option[TripEventState] = {

    val previousState = if (currentState.exists) currentState.get
      else TripEventState(mutable.Map.empty[String, TripSession])
    val map = previousState.map

    tripEvents.foreach(tripEvent=>{
      if (tripEvent.event == TripEventType.Start){
        map(tripEvent.driverId) = TripSession(tripEvent.tripEventId, tripEvent.timestamp)
      }else{
        map.remove(tripEvent.driverId)
      }
    })
    currentState.update(previousState)
    Some(currentState.get)
  }
}

object TripEventType extends Enumeration {
  type TripEventType = Value
  val Start, End = Value
}

case class GpsTick(driverId: String, timestamp: Long, gpsId: Long)

case class TripEvent(driverId: String, event: TripEventType, timestamp: Long, tripEventId: Long)

case class TickProducer(data: MemoryStream[GpsTick]) {
  private var prev = 0
  private var counter = 1

  def produce(second: Int): Unit = {
    Thread.sleep((second - prev) * 1000)
    data.addData(GpsTick("1", System.currentTimeMillis(), counter))
    counter = counter + 1
    prev = second
  }
}

case class TripProducer(data: MemoryStream[TripEvent]) {
  private var prev = 0

  def produce(second: Int, event: TripEventType, tripId: Long): Unit = {
    Thread.sleep((second - prev) * 1000)
    data.addData(TripEvent("1", event, System.currentTimeMillis(), tripId))
    prev = second
  }
}


object StreamingJoin {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
        .setMaster("local[4]")
        .setAppName("StreamingTransactionExample")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().getOrCreate()
    implicit val sqlContext: SQLContext = spark.sqlContext
    import spark.implicits._


    val gpsData: MemoryStream[GpsTick] = MemoryStream[GpsTick]
    val tripData: MemoryStream[TripEvent] = MemoryStream[TripEvent]
    val gpsProducer = Future {
      val tick = TickProducer(gpsData)
      for (i <- Range(1, 200)) {
        tick.produce(i)
      }
    }
    val tripProducer = Future {
      val tick = TripProducer(tripData)
      tick.produce(10, TripEventType.Start, 1)
      tick.produce(70, TripEventType.End, 1)
      tick.produce(100, TripEventType.Start, 2)
      tick.produce(170, TripEventType.End, 2)
    }


    val tripDf = tripData.toDF().select($"*").
        withColumn("timestampEvent", to_timestamp(col("timestamp"))).
        as[TripEvent].
        groupByKey(trip => trip.driverId).
        mapGroupsWithState(GroupStateTimeout.NoTimeout())(TripMapping.mapStreamingLogsToState).
        select(explode($"value.map")).drop("value").
        withColumnRenamed("key", "driverId")


    val joined: DataFrame = gpsData.toDF().join(tripDf, "driverId")
    val joinedDfQuery = joined.writeStream
        .trigger(Trigger.ProcessingTime("10 seconds"))
        .outputMode(OutputMode.Append())
        .format("console")
        .start

    joinedDfQuery.awaitTermination()

//    tripDf.printSchema()

//    val tripDfQuery = tripDf.writeStream
//        .trigger(Trigger.ProcessingTime("10 seconds"))
//        .outputMode(OutputMode.Update())
//        .foreachBatch { (batchDF:  Dataset[Option[TripEventState]], batchId: Long) =>
//          println(s"$batchId - ${batchDF.count()}")
//          batchDF.select(explode($"value.map")).drop("value").show()
////          batchDF.write.
////              format("json").
////              mode(SaveMode.Overwrite).
////              save(s"/home/cornel/stream5/$batchId")
//
//        }
//        .start()
//    tripDfQuery.awaitTermination()

//    val joined: DataFrame = gpsData.toDF()
//        .withColumnRenamed("driverId","gpsDriverId")
//        .withColumnRenamed("timestamp","gpsTimestamp").

//        join(
//          tripData.toDF()
//              .withColumnRenamed("driverId","tripDriverId")
//            .withColumnRenamed("timestamp","gpsTimestamp"),
//          expr("""
//            gpsDriverId = tripDriverId
//        """)
//
//        )
//
//    joined.printSchema()
//
//
//
//

  }
}
