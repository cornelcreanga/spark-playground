package com.creanga.playground.spark.example.deduplication

import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object DeduplicationConditionalExample {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("Streaming events").master("local[2]")
      .getOrCreate()
    val sc = sparkSession.sparkContext
    val structureData = Seq(
      Row("James1", "Smith1", "uid1", "1", 123),
      Row("James2", "Smith2", "uid1", "1", 124),
      Row("James3", "Smith3", "uid1", "2", 125),
      Row("James", "Smith", "uid2", "1", 126),
      Row("James", "Smith", "uid2", "1", 127),
      Row("James4", "Smith4", "uid1", "2", 128)
    )
    val structureSchema = new StructType()
      .add("firstname", StringType)
      .add("middlename", StringType)
      .add("idName", StringType)
      .add("idValue", StringType)
      .add("time", IntegerType)

    /**
     * rowid uid time code
     * 1  1      5    a
     * 2  1      6    b
     * 3  1      7    c
     * 4  2      8    a
     * 5  2      9    c
     * 6  2      9    c
     * 7  2     10    c
     * 8  2     11    a
     * 9  2     12    c
     *
     * val window = Window.partitionBy("uid").orderBy("time")
     * val result = df
     * .withColumn("lagValue", coalesce(lag(col("code"), 1).over(window), lit("")))
     * .where(
     * (col("code") !== "c") ||
     * (col("lagValue") !== "c")
     * )
     * .drop("lagValue")
     */
  }

}
