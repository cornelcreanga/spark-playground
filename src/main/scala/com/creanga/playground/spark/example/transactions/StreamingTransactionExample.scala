package com.creanga.playground.spark.example.transactions

import java.util.UUID

import com.creanga.playground.spark.util.RandomRDD
import com.creanga.playground.spark.util.Utils.randomArray
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object StreamingTransactionExample {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
        .setMaster("local[2]")
        .setAppName("StreamingTransactionExample")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)

    //in real life we'll receive this data as a stream
    val originalRdd = new RandomRDD[Item](sc,
      sc.defaultParallelism,
      sc.defaultParallelism * 200000, // will crash without enough memory (12-16gb)
      context = Map("transactionProbabilityPercent" -> "0.5",
        "maxItemsPerTransaction" -> "5",
        "transactionIncompletePercent" -> "0.01",
        "keyNo" -> "50",
        "bodyLength" -> "5000"),
      generatorFunction = generateRandomItem
    )

    val unmatchedTransactions = originalRdd.filter(item => item.isInstanceOf[TransactionalItem]).groupBy((item: Item) => {
      item match {
        case TransactionalItem(_, transactionId, _, _) => transactionId
        case _ => throw new RuntimeException("")
      }
    }).filter(item => {
      val itemsNo = item._2.head.asInstanceOf[TransactionalItem].transactionIdItems
      itemsNo > item._2.size
    }).flatMap(pair => pair._2)

    unmatchedTransactions.persist(StorageLevel.MEMORY_ONLY_SER)

    var t1 = System.currentTimeMillis()
    var count = unmatchedTransactions.count()
    var t2 = System.currentTimeMillis()
    var time = t2 - t1
    println(s"unmatchedTransactions=$count in $time")
    //write them somewhere (eg the original topic)

    t1 = System.currentTimeMillis()
    val rddToProcess = originalRdd.subtract(unmatchedTransactions)
    count = rddToProcess.count()
    t2 = System.currentTimeMillis()
    time = t2 - t1
    println(s"rddToProcess=$count in $time")

  }

  def generateRandomInt(context: Map[String, String] = Map.empty[String, String]): Array[Int] = {
    Array(scala.util.Random.nextInt(100) + 1)
  }

  def generateRandomItem(context: Map[String, String]): Seq[Item] = {
    val isTransaction = (context("transactionProbabilityPercent").toDouble > Math.random())
    val key = "key" + (context("keyNo").toInt * Math.random()).toInt + 1
    val bodyLength = context("bodyLength").toInt
    if (isTransaction) {
      val transactionIncomplete = context("transactionIncompletePercent").toDouble > Math.random()
      val transactionItems = (context("maxItemsPerTransaction").toDouble * Math.random()).toInt + 1
      val len = if (transactionIncomplete) {
        transactionItems - 1
      } else {
        transactionItems
      }
      val transactionId = UUID.randomUUID().toString
      (0 until len).view.map(counter => {
        val body = randomArray(bodyLength)
        TransactionalItem(SimpleItem(UUID.randomUUID().toString, key, body), transactionId, transactionItems, counter)
      })
    } else {
      val body = randomArray(bodyLength)
      Array(SimpleItem(UUID.randomUUID().toString, key, body))
    }
  }


}




