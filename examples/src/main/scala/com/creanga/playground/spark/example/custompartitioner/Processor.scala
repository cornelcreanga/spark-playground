package com.creanga.playground.spark.example.custompartitioner

import org.apache.spark.TaskContext

sealed trait ProcessorResult {
  val itemsNo: Long
}

case class ProcessorResultFailed(itemsNo: Long, partitionId: Int, exception: Throwable) extends ProcessorResult

case class ProcessorResultSuccessful(itemsNo: Long, partitionId: Int) extends ProcessorResult

class Processor extends Serializable {

  def processAndWriteData(taskContext: TaskContext,
                          messageIterator: Iterator[(Long, Item)]): ProcessorResult = {

    if (Math.random() > 0.5) ProcessorResultSuccessful(1, taskContext.partitionId())
    else ProcessorResultFailed(1, taskContext.partitionId(), new RuntimeException("cucu cucu"))
  }

}
