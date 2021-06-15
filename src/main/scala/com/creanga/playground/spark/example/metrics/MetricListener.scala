package com.creanga.playground.spark.example.metrics

import org.apache.spark.scheduler.{SparkListener, SparkListenerStageSubmitted, SparkListenerTaskEnd}

class MetricListener(name:String) extends SparkListener{

  var rows: Long = 0L
  var stageId = 0

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
    if (stageSubmitted.properties.getProperty("job-context") == name){
      stageId = stageSubmitted.stageInfo.stageId
    }
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    if (taskEnd.stageId == stageId)
      rows = rows + taskEnd.taskMetrics.inputMetrics.recordsRead
  }

}
