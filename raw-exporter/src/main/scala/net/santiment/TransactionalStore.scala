package net.santiment

import net.santiment.util.Store

trait Transactional {
  def begin():Unit
  def commit():Unit
  def abort():Unit
}

trait TransactionalStore[T] extends Store[T] with Transactional

class SimpleTxStore[T](writeStore:Store[T], commitStore:Store[T])
extends TransactionalStore[T]
{
  //Check if valid
  private val written: Option[T] = writeStore.read
  private val committed: Option[T] = commitStore.read

  // This is critical for our case. If something bad happens during the transaction and and we cannot recover using abort(), those two values will differ.
  // If the underlying store is persistent, we won't be able to just restart the program, but will need manual intervention
  if(written != committed) {
    val msg = s"Inconsistent state: written=$written, committed=$committed"
    throw new IllegalStateException(msg)
  }

  var last:Option[T] = _

  var txWritten:Option[T] = None

  override def begin(): Unit = {
    if (last != null) {
      throw new IllegalStateException("Transaction already started.")
    }
    last = writeStore.read

    txWritten = last
  }

  override def read: Option[T] = writeStore.read

  override def create(value:T): Unit = {
    writeStore.create(value)
    txWritten = Some(value)

    if(last == null) {
      commitStore.create(value)
    }
  }

  override def update(value:T): Unit = {
    writeStore.update(value)
    txWritten = Some(value)

    if(last == null) {
      commitStore.update(value)
    }
  }

  override def delete(): Unit = {
    writeStore.delete()

    txWritten = None

    if(last == null) {
      commitStore.delete()
    }
  }

  override def write(value:Option[T]):Option[T] = {
    val result = writeStore.write(value)

    txWritten = value

    if(last == null) {
      commitStore.write(value)
    }
    result
  }

  override def commit(): Unit = {
    if (last == null) {
      throw new IllegalStateException("Transaction not started")
    }

    if(txWritten.isEmpty && last.isDefined) {
      commitStore.delete()
    } else if (txWritten.isDefined && last.isEmpty) {
      commitStore.create(txWritten.get)
    } else if (txWritten.isDefined && last.isDefined) {
      commitStore.update(txWritten.get)
    }

    last = null
  }

  override def abort(): Unit = {
    if (last == null) {
      throw new IllegalStateException("Transaction not started")
    }
    commitStore.write(last)
    last = null
    txWritten = null
  }
}
