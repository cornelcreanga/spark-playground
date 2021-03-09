package com.creanga.playground.spark.example.transactions

import java.io.{ObjectInputStream, ObjectOutputStream}

import com.google.common.io.ByteStreams

sealed trait Item

case class SimpleItem(var id: String, var key: String, var body: Array[Byte]) extends Item with Serializable {

  private def writeObject(out: ObjectOutputStream): Unit = {
    out.writeUTF(id)
    out.writeUTF(key)
    out.write(body)
  }

  private def readObject(in: ObjectInputStream): Unit = {
    id = in.readUTF()
    key = in.readUTF()
    body = ByteStreams.toByteArray(in)
  }
}

case class TransactionalItem(var item: SimpleItem,
    var transactionId: String, //unique transaction identifier
    var transactionIdItems: Int, //no of items from a transaction
    var transactionItemId: Int //item no (used for ordering items in a transaction)
) extends Item with Serializable {


  private def writeObject(out: ObjectOutputStream): Unit = {
    out.writeObject(item)
    out.writeUTF(transactionId)
    out.writeInt(transactionIdItems)
    out.writeInt(transactionItemId)
  }

  private def readObject(in: ObjectInputStream): Unit = {
    item = in.readObject().asInstanceOf[SimpleItem]
    transactionId = in.readUTF()
    transactionIdItems = in.readInt()
    transactionItemId = in.readInt()
  }

}
