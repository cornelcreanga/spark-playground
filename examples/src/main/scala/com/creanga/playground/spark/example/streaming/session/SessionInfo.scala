package com.creanga.playground.spark.example.streaming.session

import scala.collection.mutable.ArrayBuffer

case class GpsTick(driverId: String, timestamp: Long, gpsId: String)

case class GpsTripEvent(tripEventId: String, timestamp: Long, gpsId: String)

case class TripEvent(driverId: String, event: String, timestamp: Long, tripEventId: String)

case class SessionInfo(creationTimestamp: Long, var startTimestamp: Long, var endTimestamp: Long,
                       var gpsTicks: ArrayBuffer[GpsTick])

case class SessionInfo2(tripEventId: String, var driverId: String, var startTimestamp: Long, var endTimestamp: Long,
                        var gpsTripEvents: ArrayBuffer[GpsTripEvent])

case class Activity(sessionInfo: ArrayBuffer[SessionInfo], var unassigned: ArrayBuffer[GpsTick])



