package com.creanga.playground.spark.example.streaming.session

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import com.creanga.playground.spark.example.streaming.session.TripEventType.TripEventType

object TripEventType extends Enumeration {
  type TripEventType = Value
  val Start, End = Value
}

case class GpsTick(driverId: String, timestamp: Long, gpsId: Long)

case class TripEvent(driverId: String, event: TripEventType, timestamp: Long, tripEventId: Long)

case class SessionInfo(creationTimestamp: Long, var startTimestamp: Long, var endTimestamp: Long,
    var gpsTicks: ArrayBuffer[GpsTick])

case class Activity(sessionInfo: ArrayBuffer[SessionInfo], var unassigned: ArrayBuffer[GpsTick])

case class SessionState(map: mutable.Map[String, Activity])


