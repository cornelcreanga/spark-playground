package com.creanga.playground.spark

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

import scala.util.Random

object TestParquetManyColumns {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[10]")
      // .master("local[1]")
      .getOrCreate()
    val sc = spark.sparkContext

    var t1 = System.currentTimeMillis();
    val numCols = 800
    val numRows = 800000
    val cols:Array[String] = (1 to numCols).map(i=>s"col$i").toArray
    val alphabet='a' to 'z'
    def randomString(length:Int) : String = (1 to length).map(_=>alphabet(Random.nextInt(alphabet.size))).mkString
    val randomData = sc.parallelize(1 to numRows).map{_=>(1 to numCols).map(i=> if(i%3==0) randomString(1) else "abc")}

    val df = spark.createDataFrame(randomData.map(Row.fromSeq),StructType(cols.map(name=>StructField(name,StringType))))
    df.persist()
    df.count()
    var t2 = System.currentTimeMillis()
    println (t2 - t1)
    t1 = System.currentTimeMillis();
    //snappy nu merge pe mac arm (fara munca in plus)
    df.write.option("compression","zstd").mode("overwrite").parquet("/home/cornel/manycolumns/")
    t2 = System.currentTimeMillis()
    println (t2 - t1)
    Thread.sleep(1000000)
  }

}
