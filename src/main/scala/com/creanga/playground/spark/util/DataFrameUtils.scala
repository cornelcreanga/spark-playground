package com.creanga.playground.spark.util

import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{col, lit, struct}
import org.apache.spark.sql.types.StructType

object DataFrameUtils {


  def mutate(df: DataFrame, fn: Column => Column): DataFrame = {
    df.sqlContext.createDataFrame(df.select(traverse(df.schema, fn):_*).rdd, df.schema)
  }

  def traverse(schema: StructType, fn: Column => Column, path: String = ""): Array[Column] = {
    schema.fields.map(f => {
      f.dataType match {
        case s: StructType => struct(traverse(s, fn, path + f.name + "."): _*)
        case _ => fn(col(path + f.name))
      }
    })
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

  def unionExpr(myCols: Seq[String], allCols: Seq[String]): Seq[org.apache.spark.sql.Column] = {
    allCols.toList.map {
      case x if myCols.contains(x) => col("`" + x + "`")
      case x => lit(null).as(x)
    }
  }



}
