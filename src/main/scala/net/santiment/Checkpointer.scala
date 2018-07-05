package net.santiment

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.ExponentialBackoffRetry


trait Checkpointer[T <: Serializable] {
  def read:Option[T]
  def create(value:T): Unit
  def update(value:T): Unit

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

class ZookeepeerCheckpointer[T <:Serializable](config:ZookeeperCheckpointerConfig) extends Checkpointer[T] {

  override def read: Option[T] = {
    if (client.checkExists.forPath(config.path) != null) {
      val bytes = client.getData.forPath(config.path)
      Some(fromByteArray(bytes))
    }
    else {
      None
    }
  }

  override def update(value:T): Unit = {
    client.setData().forPath(config.path, toByteArray(value))
  }

  override def create(value:T): Unit = {
    client.create().creatingParentsIfNeeded().forPath(config.path, toByteArray(value))
  }

  lazy val retryPolicy = new ExponentialBackoffRetry(1000, 10)

  lazy val client: CuratorFramework = {
    val result = CuratorFrameworkFactory.builder()
      .namespace(config.namespace)
      .connectString(config.connectionString)
      .retryPolicy(retryPolicy)
      .build()

    result.start()
    result.blockUntilConnected()
    result
  }

}