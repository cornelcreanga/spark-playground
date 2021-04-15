package com.creanga.playground.spark.example.union

import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.{SparkConf, SparkContext}

object FlattenedUnionExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
        .setMaster("local[2]")
        .setAppName("StreamingTransactionExample")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().getOrCreate()

    val structureData1 = Seq(
      Row(Row("James ","","Smith", Row(13)),"36636","M",3100),
      Row(Row("Robert ","","Williams"),"42114","M",1400),
      Row(Row("Maria ","Anne","Jones"),"39192","F",5500),
      Row(Row("Jen","Mary","Brown"),"","F",-1)
    )

    val structureData2 = Seq(
      Row(Row("James ","","Smith", Row(13)),"36636","M",4100),
      Row(Row("Michael ","Rose","", Row(22)),"40288","M",4300),
      Row(Row("Robert ","","Williams"),"42114","M",1400),
      Row(Row("Maria ","Anne","Jones"),"39192","F",5500),
      Row(Row("Jen","Mary","Brown"),"","F",-1)
    )


    val structureSchema1 = new StructType()
        .add("name",new StructType()
            .add("firstname",StringType)
            .add("middlename",StringType)
            .add("id",new StructType().add("crmid", StringType))
        )
        .add("id",StringType)
        .add("gender",StringType)
        .add("salary",IntegerType)

    val structureSchema2 = new StructType()
        .add("name",new StructType()
            .add("middlename",StringType)
            .add("lastname",StringType)
            .add("id",new StructType().add("eccid", StringType))
        )
        .add("id",StringType)
        .add("gender",StringType)
        .add("salary",IntegerType)


    val df1 = spark.createDataFrame(sc.parallelize(structureData1),structureSchema1)
    df1.printSchema()
    val df2 = spark.createDataFrame(sc.parallelize(structureData1),structureSchema2)
    df2.printSchema()

    val flattened1 = df1.select(flattenSchema(df1.schema):_*)
    val flattened2 = df2.select(flattenSchema(df2.schema):_*)
    flattened1.printSchema()
    flattened2.printSchema()
    val union = unionWithDifferentSchema(List(flattened1, flattened2), spark)
    union.printSchema()


  }

  def flattenSchema(schema: StructType, prefix: String = null) : Array[Column] = {
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
    val escapedColumns = allColumns.map(c=>"`"+c+"`")

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
