package com.creanga.playground.spark.example.union

import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

//https://github.com/AbsaOSS/spark-hats
object UnionExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("StreamingTransactionExample")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().getOrCreate()

    val structureData1 = Seq(
      Row(Row("James ", "Smith", Row("13")), "36636", "M", 3100),
      Row(Row("Robert ", "Williams", Row("13")), "42114", "M", 1400),
      Row(Row("Maria ", "Jones", Row("13")), "39192", "F", 5500),
      Row(Row("Jen", "Brown", Row("")), "", "F", 100)
    )

    val structureSchema1 = new StructType()
      .add("name", new StructType()
        .add("firstname", StringType)
        .add("middlename", StringType)
        .add("crmid", new StructType().add("name", StringType))
      )
      .add("id", StringType)
      .add("gender", StringType)
      .add("salary", IntegerType)

    val mergedSchema = new StructType()
      .add("name", new StructType()
        .add("firstname", StringType)
        .add("middlename", StringType)
        .add("crmid", new StructType()
          .add("name", StringType)
          .add("value", new StructType()
            .add("data1", StringType)
            .add("data2", new ArrayType(
              new StructType()
                .add("data3", LongType)
                .add("data4", LongType),
              containsNull = false)
            )
          )
        )
      )
      .add("id", StringType)
      .add("gender", StringType)
      .add("salary", IntegerType)

    val df1 = spark.createDataFrame(sc.parallelize(structureData1), structureSchema1)
    df1.printSchema()


    println(df1.schema.names.mkString("\n"))


  }


}
