package com.creanga.playground.spark.example.deduplication

import java.io.{ObjectInputStream, ObjectOutputStream}

case class Item(var id: String,
    var timestamp: String) extends Serializable { //don't know how to easily generate unique timestamps around partitions
  private def writeObject(out: ObjectOutputStream): Unit = {
    out.writeUTF(id)
    out.writeUTF(timestamp)
  }

  private def readObject(in: ObjectInputStream): Unit = {
    id = in.readUTF()
    timestamp = in.readUTF()
  }
}
