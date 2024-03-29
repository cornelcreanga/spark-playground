package com.creanga.playground.spark.example.testcase

import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.{SparkConf, SparkContext, TaskContext}

import java.util

object DecomissionTest {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    val jsc = new JavaSparkContext(sc)

    while (true) {
      for (i <- 1 to 2) {
        val thread: Thread = new Thread {
          override def run(): Unit = {
            val local = i;
            val taskTime: Long = (10 + Math.random * 20).toLong * 1000
            jsc.parallelize(new util.ArrayList[AnyRef]).foreachPartition((_: util.Iterator[AnyRef]) => {
              System.err.println(local + " If you’re feeling bad, you just look at the cats, you’ll feel better, because they know that everything is, just as it is. There’s nothing to get excited about. They just know. They’re saviours. I'll wait " + taskTime + " ms")
              Thread.sleep(taskTime)
            })
          }
        }
        thread.start()
      }

      Thread.sleep(60000)
    }
  }

}
