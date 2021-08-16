package com.creanga.playground.spark.util

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, SparkContext, TaskContext}

class RandomRDD[A: ClassTag](@transient private val sc: SparkContext,
    numSlices: Int,
    numValues: Int,
    context: Map[String, String] = Map.empty[String, String],
    generatorFunction: Map[String, String] => Seq[A]) extends RDD[A](sc, deps = Seq.empty) {

  private val valuesPerSlice = numValues / numSlices
  private val slicesWithExtraItem = numValues % numSlices

  override def compute(split: Partition, context: TaskContext): Iterator[A] =
    split.asInstanceOf[RandomPartition[A]].values

  override protected def getPartitions: Array[Partition] = {
    //distribute data evenly
    ((0 until slicesWithExtraItem).view.map(new RandomPartition[A](_, valuesPerSlice + 1, context, generatorFunction)) ++
        (slicesWithExtraItem until numSlices).view.map(new RandomPartition[A](_, valuesPerSlice, context, generatorFunction))).toArray
  }

}

class RandomPartition[A: ClassTag](val index: Int,
    numValues: Int,
    context: Map[String, String] = Map.empty[String, String],
    generatorFunction: (Map[String, String]) => Seq[A]) extends Partition {

  if (context.contains("partitionIndex"))
    throw new RuntimeException("partitionIndex is an reserved keyword")

  def values: Iterator[A] = {
    val list = new ArrayBuffer[A]()
    for (_ <- 0 until numValues) {
      list ++= generatorFunction(context + ("partitionIndex" -> index.toString))
    }
    list.iterator
  }
}

