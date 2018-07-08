package net.santiment

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

import com.typesafe.scalalogging.LazyLogging
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.ExponentialBackoffRetry


trait Store[T <: java.io.Serializable] {
  def create(value:T): Unit
  def read:Option[T]
  def update(value:T): Unit
  def delete(): Unit

  def write(value:Option[T]): Option[T] = {
    val old = read

    if(old.isEmpty && value.isEmpty) {
      return old
    }

    if(old.isDefined && value.isEmpty) {
      delete()
      return old
    }

    if(old.isEmpty && value.isDefined) {
      create(value.get)
      return old
    }

    if(old.isDefined && value.isDefined) {
      update(value.get)
      return old
    }

    old
  }

  def toByteArray(value:T):Array[Byte] = {
    val bos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(bos)
    oos.writeObject(value)
    oos.close()
    bos.toByteArray
  }

  def fromByteArray(bytes:Array[Byte]):T = {
    val is = new ByteArrayInputStream(bytes)
    val ois = new ObjectInputStream(is)
    val result = ois.readObject().asInstanceOf[T]
    ois.close()
    result
  }
}

class ZookeeperStore[T <:java.io.Serializable](private val client:CuratorFramework, path:String)
  extends Store[T]
    with LazyLogging {

  override def read: Option[T] = {
    if (client.checkExists.forPath(path) != null) {
      val bytes = client.getData.forPath(path)
      Some(fromByteArray(bytes))
    }
    else {
      None
    }
  }

  override def update(value:T): Unit = {
    client.setData().forPath(path, toByteArray(value))
  }

  override def create(value:T): Unit = {
    client.create().creatingParentsIfNeeded().forPath(path, toByteArray(value))
  }

  override def delete(): Unit = {
    client.delete().deletingChildrenIfNeeded().forPath(path)
  }
}
