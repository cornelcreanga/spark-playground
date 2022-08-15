package com.creanga.playground.spark.util;

import scala.Serializable;

import java.util.List;
import java.util.Map;
public interface RecordGenerator<T> extends Serializable {
    List<T> generate(Map<String, Object> context, int itemNumber);
}
