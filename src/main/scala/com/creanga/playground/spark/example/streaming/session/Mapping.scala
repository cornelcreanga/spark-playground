package com.creanga.playground.spark.example.streaming.session

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.{Duration, MINUTES}
import scala.util.control.Breaks._

import com.creanga.playground.spark.util.Mapper.plainMapper
import org.apache.spark.sql.Row
import org.apache.spark.sql.streaming.GroupState

object Mapping {

  val watermark: Duration = Duration(1, MINUTES)

  /**
   * assumptions:
   * no missing tripStart/tripEnd events
   * @return
   */
  def filterNonSessionGpsEvents(driverId: String,
      rows: Iterator[Row],
      currentState: GroupState[SessionState]): Iterator[GpsTick] = {

    val gpsToReturn = new ArrayBuffer[GpsTick](1024)
    val unassigned = new ArrayBuffer[GpsTick]()

    val previousState: SessionState = if (currentState.exists) {
      currentState.get
    } else {
      val map = new mutable.HashMap[String, Activity]()
      map(driverId) = Activity(new ArrayBuffer[SessionInfo], ArrayBuffer.empty[GpsTick])
      SessionState(map)
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
    val startEvents = tripEvents.filter(t => t.event == TripEventType.Start).sortBy(_.timestamp)
    val endEvents = tripEvents.filter(t => t.event == TripEventType.End).sortBy(_.timestamp)

    val activity = previousState.map.getOrElse(driverId,
      Activity(new ArrayBuffer[SessionInfo], ArrayBuffer.empty[GpsTick]))
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

    val currentTimestamp = System.currentTimeMillis()
    gpsEvents ++= activity.unassigned
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
        if ((!found) && (gpsEvent.timestamp > currentTimestamp - watermark.toMillis)) {
          //println(s"unassigned $gpsEvent")
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
