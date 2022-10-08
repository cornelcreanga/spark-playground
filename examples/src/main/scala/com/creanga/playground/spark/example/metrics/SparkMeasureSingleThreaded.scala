package com.creanga.playground.spark.example.metrics

import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

object SparkMeasureSingleThreaded {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Streaming events").master("local[2]")
      .getOrCreate()
    val sc = spark.sparkContext

    val structureData1 = Seq(
      Row(Row("James ", "Smith", Row("13")), "36636", "M", 3100),
      Row(Row("Robert ", "Williams", Row("")), "42114", "M", 1400),
      Row(Row("Maria ", "Jones", Row("")), "39192", "F", 5500),
      Row(Row("Jen", "Brown", Row("")), "", "F", -1)
    )

    val structureSchema1 = new StructType()
      .add("name", new StructType()
        .add("firstname", StringType)
        .add("middlename", StringType)
        .add("id", new StructType().add("crmid", StringType))
      )
      .add("id", StringType)
      .add("gender", StringType)
      .add("salary", IntegerType)

    val df1 = spark.createDataFrame(sc.parallelize(structureData1), structureSchema1)

    //spark.newSession()
    val stageMetrics = ch.cern.sparkmeasure.StageMetrics(spark)
    stageMetrics.begin()
    df1.write.
      format("json").
      mode(SaveMode.Overwrite).
      save("/tmp/temp/")

    stageMetrics.end()
    var map = stageMetrics.reportMap()
    println(map.get("recordsWritten"))
    //stageMetrics.printReport()

    stageMetrics.begin()
    val df2 = spark.read.json("/tmp/temp")
    stageMetrics.end()
    map = stageMetrics.reportMap()
    println(map.get("recordsRead"))


    Thread.sleep(100000)
    //stageMetrics.runAndMeasure(spark.sql("select count(*) from range(1000) cross join range(1000) cross join range(1000)").show())
  }

}
