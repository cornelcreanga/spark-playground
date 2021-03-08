package com.creanga.playground.spark.util

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, SparkContext, TaskContext}

class RandomPartition[A: ClassTag](val index: Int,
    numValues: Int,
    context: Map[String, String] = Map.empty[String, String],
    generatorFunction: (Map[String, String]) => Seq[A]) extends Partition {

  def values: Iterator[A] = {
    var list = new ArrayBuffer[A]()
    for (_ <- 0 until numValues) {
      list ++= generatorFunction(context)
    }
    list.iterator
  }
}

class RandomRDD[A: ClassTag](@transient private val sc: SparkContext,
    numSlices: Int,
    numValues: Int,
    context: Map[String, String] = Map.empty[String, String],
    generatorFunction: Map[String, String] => Seq[A]) extends RDD[A](sc, deps = Seq.empty) {

  private val valuesPerSlice = numValues / numSlices
  private val slicesWithExtraItem = numValues % numSlices

  // Just ask the partition for the data
  override def compute(split: Partition, context: TaskContext): Iterator[A] =
    split.asInstanceOf[RandomPartition[A]].values

  // Generate the partitions so that the load is as evenly spread as possible
  // e.g. 10 partition and 22 items -> 2 slices with 3 items and 8 slices with 2
  override protected def getPartitions: Array[Partition] = {
    ((0 until slicesWithExtraItem).view.map(new RandomPartition[A](_, valuesPerSlice + 1, context, generatorFunction)) ++
        (slicesWithExtraItem until numSlices).view.map(new RandomPartition[A](_, valuesPerSlice, context, generatorFunction))).toArray
  }

}
