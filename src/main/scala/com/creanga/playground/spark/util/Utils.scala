package com.creanga.playground.spark.util

object Utils {
  def randomArray(n: Int): Array[Byte] = {
    //val body = new Array[Byte](bodyLength)
    //Random.nextBytes(body) - todo - check the speed on random implementation before using it
    Array.fill[Byte](n)(0)
  }

}
