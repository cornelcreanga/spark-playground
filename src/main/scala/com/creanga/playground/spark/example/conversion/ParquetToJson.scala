package com.creanga.playground.spark.example.conversion

import org.apache.spark.sql.{SaveMode, SparkSession}

object ParquetToJson {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
        .appName("Streaming events").master("local[2]")
        .getOrCreate()
    val sc = spark.sparkContext
    val df = spark.read.load("/tmp/incrementalDataframe9079194238659646622.tmp/part-00000-91fc6e7d-c305-4790-a365-e1ae198d1855-c000.snappy.parquet")
    df.write.
        format("json").
        mode(SaveMode.Overwrite).
        save("/home/cornel/incremental/")
  }

}
