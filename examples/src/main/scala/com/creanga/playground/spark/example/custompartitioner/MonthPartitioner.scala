package com.creanga.playground.spark.example.custompartitioner

import org.apache.spark.Partitioner

import java.time.{Instant, ZoneOffset}

class MonthPartitioner() extends Partitioner {

  override def numPartitions: Int = 12

  override def getPartition(key: Any): Int = key match {

    case key: Long =>
      Instant.ofEpochMilli(key).atZone(ZoneOffset.UTC).toLocalDate.getMonthValue - 1

  }
}
