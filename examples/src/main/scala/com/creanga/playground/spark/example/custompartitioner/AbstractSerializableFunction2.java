package com.creanga.playground.spark.example.custompartitioner;

import scala.runtime.AbstractFunction2;

import java.io.Serializable;

public abstract class AbstractSerializableFunction2<T1, T2, R> extends AbstractFunction2<T1, T2, R> implements Serializable {

}
