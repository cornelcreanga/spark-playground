package com.creanga.playground.spark.example.transformations

import com.creanga.playground.spark.util.DataFrameUtils.mutate
import org.apache.spark.sql.functions.lit
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{ArrayType, IntegerType, LongType, StringType, StructType}

object UpdateNestedColumn {
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
        .add("name",new StructType()
            .add("firstname",StringType)
            .add("middlename",StringType)
            .add("crmid",new StructType().add("name", StringType))
        )
        .add("id",StringType)
        .add("gender",StringType)
        .add("salary",IntegerType)

    val df1 = spark.createDataFrame(sc.parallelize(structureData1), structureSchema1)
    df1.printSchema()
    df1.show(truncate=false)
    val df2 = mutate(df1,  c => {if (c.toString == "name.crmid.name") lit("3333") else c})
    df2.show(truncate=false)

    val index = df1.schema.fieldIndex("name")
    val isPresent = df1.schema(index).dataType.asInstanceOf[StructType]
        .fieldNames.contains("firstname2")
    println(isPresent)



  }
}
