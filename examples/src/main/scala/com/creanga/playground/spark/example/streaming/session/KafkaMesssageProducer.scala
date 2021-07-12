package com.creanga.playground.spark.example.streaming.session

import java.util.UUID
import java.util.concurrent.Executors

import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService, Future}
import scala.concurrent.duration.Duration
import scala.util.control.NonFatal

import com.creanga.playground.spark.util.KafkaMessageProducer
import com.creanga.playground.spark.util.Mapper.plainMapper
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}

object KafkaMesssageProducer {


  implicit val singleThreadContext: ExecutionContextExecutorService =
    ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(128))

  case class TripGenerator(driverId: String, durationInMins: Int, gpsEventInterval: Int,
      producer: KafkaProducer[Array[Byte], Array[Byte]]) {

    def createTrip(): Unit = {
      val tripId = UUID.randomUUID().toString
      var tripEvent = TripEvent(driverId, "Start", System.currentTimeMillis(), tripId)
      producer.send(new ProducerRecord("tripEvents", plainMapper.writeValueAsBytes(tripEvent)))
//      producer.send(new ProducerRecord("tripEvents", plainMapper.writeValueAsBytes(tripEvent)),
//        new Callback() {
//          override def onCompletion(recordMetadata: RecordMetadata, e: Exception): Unit = {
//            if (e != null) {
//              e.printStackTrace()
//            }
//            else {
//              println("done")
//            }
//          }
//        })

      producer.flush()

      val tripDuration = Math.max(Math.round(Math.random() * durationInMins), 1).toInt
      println(s"driverId $driverId tripDuration $tripDuration")
      for (i <- 1 to (tripDuration * 6)) {
        val gpsTick = GpsTripEvent(tripId, System.currentTimeMillis(), UUID.randomUUID().toString)
        producer.send(new ProducerRecord("gpsEvents", plainMapper.writeValueAsBytes(gpsTick)))
        producer.flush()
        Thread.sleep(Duration("10 seconds").toMillis)
      }
      tripEvent = TripEvent(driverId, "End", System.currentTimeMillis(), tripId)
      producer.send(new ProducerRecord("tripEvents", plainMapper.writeValueAsBytes(tripEvent)))
      producer.flush()

    }

  }

  def main(args: Array[String]): Unit = {
    val trips = 100
    val duration = 10
    for (i <- 1 to trips) {
      Future {
        try {
          val tripGenerator = TripGenerator(i.toString, duration, 1, KafkaMessageProducer.kafkaProducer)
          tripGenerator.createTrip()
        }catch {
          case NonFatal(e) => e.printStackTrace() //todo
        }
      }
    }

  }

}
