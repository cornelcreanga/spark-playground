package com.creanga.playground.spark.example.union

import org.apache.spark.sql.functions.{col, lit, typedLit}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
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
      Row(Row("James ", "", "Smith", Row(13)), "36636", "M", 3100),
      Row(Row("Robert ", "", "Williams"), "42114", "M", 1400),
      Row(Row("Maria ", "Anne", "Jones"), "39192", "F", 5500),
      Row(Row("Jen", "Mary", "Brown"), "", "F", -1)
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
    //df1.printSchema()
    println(df1.schema.names.mkString("\n"))

    df1.withColumn("d",lit(null).cast(StringType))


  }

  def flattenSchema(schema: StructType, prefix: String = null): Array[Column] = {
    schema.fields.flatMap(f => {
      val colName = if (prefix == null) f.name else (prefix + "." + f.name)

      f.dataType match {
        case st: StructType => flattenSchema(st, colName)
        case _ => Array(col(colName).alias(colName))
      }
    })
  }

  def unionWithDifferentSchema(dataframes: List[DataFrame], spark: SparkSession): DataFrame = {
    val allColumns: Array[String] = dataframes.map(_.columns).reduce((x, y) => x.union(y)).distinct
    val escapedColumns = allColumns.map(c => "`" + c + "`")

    val masterSchema = StructType(dataframes.map(_.schema.fields).reduce((x, y) => x.union(y)).groupBy(_.name.toUpperCase).map(_._2.head).toArray)
    val masterEmptyDF = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], masterSchema).select(escapedColumns.head, escapedColumns.tail: _*)

    dataframes.map(df => df.select(unionExpr(df.columns, allColumns): _*)).foldLeft(masterEmptyDF)((x, y) => x.union(y))
  }

  private def unionExpr(myCols: Seq[String], allCols: Seq[String]): Seq[org.apache.spark.sql.Column] = {
    allCols.toList.map {
      case x if myCols.contains(x) => col("`" + x + "`")
      case x => lit(null).as(x)
    }
  }

}
