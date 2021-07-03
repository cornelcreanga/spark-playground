package com.creanga.playground.spark.example.streaming.session

import java.util.UUID
import java.util.concurrent.Executors

import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService, Future}
import scala.concurrent.duration.Duration

import com.creanga.playground.spark.util.KafkaMessageProducer
import com.creanga.playground.spark.util.Mapper.plainMapper
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object MesssageProducer {


  implicit val singleThreadContext: ExecutionContextExecutorService =
    ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(128))

  case class TripGenerator(driverId: String, durationInMins: Int, gpsEventInterval: Int,
      producer: KafkaProducer[Array[Byte], Array[Byte]]) {

    def createTrip(): Unit = {
      val tripId = UUID.randomUUID().toString
      val tripEvent = TripEvent(driverId, TripEventType.Start, System.currentTimeMillis(), tripId)
      producer.send(new ProducerRecord[Array[Byte], Array[Byte]]("tripEvents", plainMapper.writeValueAsBytes(tripEvent)))
      val tripDuration = Math.max( Math.random() * durationInMins, 5).toInt
      for (i <- 1 to (tripDuration*6)){
        val gpsTick = GpsTick(driverId, System.currentTimeMillis(), UUID.randomUUID().toString)
        producer.send(new ProducerRecord[Array[Byte], Array[Byte]]("gpsEvents", plainMapper.writeValueAsBytes(gpsTick)))
        Thread.sleep(Duration("10 seconds").toMillis)
      }
    }

  }

  def main(args: Array[String]): Unit = {
    val trips = 100
    val duration = 30
    for (i <- 1 to trips){
      Future{
        val tripGenerator = TripGenerator(i.toString, duration , 1, KafkaMessageProducer.kafkaProducer)
        tripGenerator.createTrip()
      }
    }

  }

}
