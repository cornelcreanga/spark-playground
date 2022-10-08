package com.creanga.playground.spark.example.deduplication

import com.google.common.io.ByteStreams

import java.io.{ObjectInputStream, ObjectOutputStream}

case class Item(var id: String, var timestamp: String, var body: Array[Byte]) extends Serializable { //don't know how to easily generate unique timestamps around partitions
  private def writeObject(out: ObjectOutputStream): Unit = {
    out.writeUTF(id)
    out.writeUTF(timestamp)
    out.write(body)
  }

  private def readObject(in: ObjectInputStream): Unit = {
    id = in.readUTF()
    timestamp = in.readUTF()
    body = ByteStreams.toByteArray(in)
  }
}
