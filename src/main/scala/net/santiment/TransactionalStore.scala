package net.santiment

trait Transactional {
  def begin():Unit
  def commit():Unit
  def abort():Unit
}

trait TransactionalStore[T <: java.io.Serializable] extends Store[T] with Transactional

class SimpleTxStore[T <: java.io.Serializable](writeStore:Store[T], commitStore:Store[T])
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
  override def begin(): Unit = {
    if (last != null) {
      throw new IllegalStateException("Transaction already started.")
    }
    last = writeStore.read
  }

  override def read: Option[T] = writeStore.read

  override def create(value:T): Unit = {
    writeStore.create(value)
    if(last == null) {
      commitStore.create(value)
    }
  }

  override def update(value:T): Unit = {
    writeStore.update(value)
    if(last == null) {
      commitStore.update(value)
    }
  }

  override def delete(): Unit = {
    writeStore.delete()
    if(last == null) {
      commitStore.delete()
    }
  }

  override def write(value:Option[T]):Option[T] = {
    val result = writeStore.write(value)
    if(last == null) {
      commitStore.write(value)
    }
    result
  }

  override def commit(): Unit = {
    if (last == null) {
      throw new IllegalStateException("Transaction not started")
    }
    commitStore.write(writeStore.read)
    last = null
  }

  override def abort(): Unit = {
    if (last == null) {
      throw new IllegalStateException("Transaction not started")
    }
    commitStore.write(last)
    last = null
  }
}
