package net.santiment

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import java.nio.ByteBuffer

import com.typesafe.scalalogging.LazyLogging
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.ExponentialBackoffRetry

trait Serde[T] {
  def toByteArray(value:T):Array[Byte]
  def fromByteArray(bytes:Array[Byte]):T
}

trait Store[T] {
  def create(value:T): Unit
  def read:Option[T]
  def update(value:T): Unit
  def delete(): Unit

  def write(value:Option[T]): Option[T] = {
    val old = read

    if(old.isDefined && value.isEmpty) delete()

    else if(old.isEmpty && value.isDefined) create(value.get)

    else if(old.isDefined && value.isDefined) update(value.get)

    old
  }
}

class ZookeeperStore[T](private val client:CuratorFramework, path:String)(implicit serde:Serde[T])
  extends Store[T]
    with LazyLogging {

  override def read: Option[T] = {
    if (client.checkExists.forPath(path) != null) {
      val bytes = client.getData.forPath(path)
      Some(serde.fromByteArray(bytes))
    }
    else {
      None
    }
  }

  override def update(value:T): Unit = {
    client.setData().forPath(path, serde.toByteArray(value))
  }

  override def create(value:T): Unit = {
    client.create().creatingParentsIfNeeded().forPath(path, serde.toByteArray(value))
  }

  override def delete(): Unit = {
    client.delete().deletingChildrenIfNeeded().forPath(path)
  }
}

object Store {
  implicit def serializableToSerde[T <: java.io.Serializable]: Serde[T] = new Serde[T] {
    override def toByteArray(value: T): Array[Byte] = {
      val bos = new ByteArrayOutputStream()
      val oos = new ObjectOutputStream(bos)
      oos.writeObject(value)
      oos.close()
      bos.toByteArray
    }

    override def fromByteArray(bytes: Array[Byte]): T = {
      val is = new ByteArrayInputStream(bytes)
      val ois = new ObjectInputStream(is)
      val result = ois.readObject().asInstanceOf[T]
      ois.close()
      result
    }
  }

  implicit def IntSerde:Serde[Int] = new Serde[Int] {
    override def toByteArray(value: Int): Array[Byte] = {
      val buffer = ByteBuffer.allocate(4)
      buffer.putInt(value)
      buffer.array()
    }

    override def fromByteArray(bytes: Array[Byte]): Int = {
      val buffer = ByteBuffer.wrap(bytes)
      buffer.getInt
    }
  }

}
