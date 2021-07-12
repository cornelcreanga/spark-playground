package com.creanga.playground.spark.example.streaming.session

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.{Duration, MINUTES}
import scala.util.control.Breaks._

import com.creanga.playground.spark.util.Mapper.plainMapper
import org.apache.spark.sql.Row
import org.apache.spark.sql.streaming.GroupState

object Mapping {


  def buildSessions(tripId: String, rows: Iterator[Row],
      currentState: GroupState[SessionInfo2]): SessionInfo2 = {

    val previousState: SessionInfo2 = if (currentState.exists) {
      currentState.get
    } else {
      SessionInfo2(tripId,"", 0L, 0L, ArrayBuffer.empty[GpsTripEvent])
    }
    val tripEvents = new ArrayBuffer[TripEvent]
    val gpsEvents = new ArrayBuffer[GpsTripEvent]

    rows.foreach(row => {
      val json = row.getAs[String]("value")
      if (json.contains("driverId")) {
        tripEvents += plainMapper.readValue(json, classOf[TripEvent])
      } else {
        gpsEvents += plainMapper.readValue(json, classOf[GpsTripEvent])
      }
    })
    val startEvent = tripEvents.find(t => t.event == "Start")
    val endEvent = tripEvents.find(t => t.event == "End")
    if (startEvent.isDefined){
      previousState.startTimestamp = startEvent.get.timestamp
      previousState.driverId = startEvent.get.driverId
    }
    if (endEvent.isDefined){
      previousState.endTimestamp = endEvent.get.timestamp
    }
    gpsEvents.foreach(e=>{
      previousState.gpsTripEvents += e
    })
    currentState.update(previousState)
    previousState
  }

  /**
   * assumptions:
   * no missing tripStart/tripEnd events
   *
   * @return
   */
  def filterNonSessionGpsEvents(driverId: String,
      rows: Iterator[Row],
      currentState: GroupState[Activity]): Iterator[GpsTick] = {

    val gpsToReturn = new ArrayBuffer[GpsTick](1024)
    val unassigned = new ArrayBuffer[GpsTick]()

    val previousState: Activity = if (currentState.exists) {
      currentState.get
    } else {
      Activity(new ArrayBuffer[SessionInfo], ArrayBuffer.empty[GpsTick])
    }

    val tripEvents = new ArrayBuffer[TripEvent]
    val gpsEvents = new ArrayBuffer[GpsTick]

    rows.foreach(row => {
      val json = row.getAs[String]("value")
      if (json.contains("tripEventId")) {
        tripEvents += plainMapper.readValue(json, classOf[TripEvent])
      } else {
        gpsEvents += plainMapper.readValue(json, classOf[GpsTick])
      }
    })
    val startEvents = tripEvents.filter(t => t.event == "Start").sortBy(_.timestamp)
    val endEvents = tripEvents.filter(t => t.event == "End").sortBy(_.timestamp)
    val lastEndEventTimestamp = if (endEvents.nonEmpty) {
      endEvents.last.timestamp
    } else if (startEvents.nonEmpty) {
      startEvents.last.timestamp
    } else 0L

    val activity = previousState
    val sessions = activity.sessionInfo

    startEvents.foreach(e => {
      sessions += SessionInfo(System.currentTimeMillis(), e.timestamp, 0L, new ArrayBuffer[GpsTick])
    })
    endEvents.foreach(endEvent => {
      breakable {
        sessions.foreach(s => {
          if (s.endTimestamp == 0) {
            s.endTimestamp = endEvent.timestamp
            break;
          }
        })
      }
    })

    if (activity.unassigned.nonEmpty) {
      gpsEvents.insertAll(0, activity.unassigned)
    }
    gpsEvents.foreach(gpsEvent => {
      breakable {
        var found = false
        sessions.foreach(s => {
          if (gpsEvent.timestamp >= s.startTimestamp && ((gpsEvent.timestamp <= s.endTimestamp) || (s.endTimestamp == 0))) {
            //println(s"found $gpsEvent")
            gpsToReturn += gpsEvent
            found = true
            break
          }
        })
        if ((!found) && (gpsEvent.timestamp > lastEndEventTimestamp)) {
          println(s"unassigned $gpsEvent")
          unassigned += gpsEvent
        }
      }
    })
    if (unassigned.size != activity.unassigned.size) {
      activity.unassigned = unassigned
    }
    //we might want to expire the old sessions too todo
    currentState.update(previousState)
    gpsToReturn.iterator
  }

}
