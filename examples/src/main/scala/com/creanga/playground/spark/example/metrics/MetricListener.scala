package com.creanga.playground.spark.example.metrics

import java.util.concurrent.atomic.AtomicLong
import scala.collection.mutable
import com.creanga.playground.spark.example.metrics.MetricListener.JOB_CONTEXT_NAME
import org.apache.spark.metrics.{ExecutorMetricType, JVMHeapMemory}
import org.apache.spark.scheduler.{SparkListener, SparkListenerExecutorMetricsUpdate, SparkListenerJobEnd, SparkListenerStageSubmitted, SparkListenerTaskEnd}
import org.json4s.JsonAST.JField

class MetricListener(jobContextValue: String) extends SparkListener {

  override def onExecutorMetricsUpdate(executorMetricsUpdate: SparkListenerExecutorMetricsUpdate): Unit = {
    println("onExecutorMetricsUpdate")
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = super.onJobEnd(jobEnd)

  var rowsRead: AtomicLong = new AtomicLong
  var bytesRead: AtomicLong = new AtomicLong
  var rowsWritten: AtomicLong = new AtomicLong
  var bytesWritten: AtomicLong = new AtomicLong
  var stageIds = new mutable.HashSet[Int]()

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
    if (stageSubmitted.properties.getProperty(JOB_CONTEXT_NAME) == jobContextValue) {
      stageIds.add(stageSubmitted.stageInfo.stageId)
    }
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {


//
//    val execMetrics = taskEnd.taskExecutorMetrics
//    val l = execMetrics.getMetricValue("JVMHeapMemory");
    if (stageIds.contains(taskEnd.stageId)) {
      if (taskEnd.taskMetrics != null) {//taskMetrics is null for a failed task
        rowsRead.addAndGet(taskEnd.taskMetrics.inputMetrics.recordsRead)
        bytesRead.addAndGet(taskEnd.taskMetrics.inputMetrics.bytesRead)
        rowsWritten.addAndGet(taskEnd.taskMetrics.outputMetrics.recordsWritten)
        bytesWritten.addAndGet(taskEnd.taskMetrics.outputMetrics.bytesWritten)
      }
    }
  }
}

object MetricListener {
  val JOB_CONTEXT_NAME = "job-context"
}
