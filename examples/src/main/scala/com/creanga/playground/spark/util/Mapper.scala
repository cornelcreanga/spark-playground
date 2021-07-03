package com.creanga.playground.spark.util

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper

object Mapper {

  val plainMapper: ObjectMapper = buildPlainObjectMapper()

  private def buildPlainObjectMapper(): ObjectMapper = {
    val mapper = (new ObjectMapper() with ScalaObjectMapper)
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    mapper.enable(DeserializationFeature.USE_LONG_FOR_INTS)
    mapper.setSerializationInclusion(Include.NON_NULL)
    mapper.registerModule(DefaultScalaModule)
    mapper.setSerializationInclusion(JsonInclude.Include.NON_ABSENT)
  }
}
