package net.santiment

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

import com.typesafe.scalalogging.LazyLogging
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.ExponentialBackoffRetry


trait Checkpointer[T <: java.io.Serializable] {
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

class ZookeeperCheckpointer[T <:java.io.Serializable](config:ZookeeperCheckpointerConfig)
  extends Checkpointer[T]
    with LazyLogging {

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

  def close(): Unit = {
    client.close()
    logger.info("Zookeeper connection closed")
  }

  lazy val retryPolicy = new ExponentialBackoffRetry(1000, 10)

  lazy val client: CuratorFramework = {
    logger.debug(s"Building Zookeeper client")
    val result = CuratorFrameworkFactory.builder()
      .namespace(config.namespace)
      .connectString(config.connectionString)
      .retryPolicy(retryPolicy)
      .build()

    logger.debug(s"Connecting to Zookeeper at ${config.connectionString}")
    result.start()
    logger.debug(s"Blocking until connected")
    result.blockUntilConnected()
    logger.info(s"Connected to Zookeeper at ${config.connectionString}. Namespace: ${config.namespace}")
    result
  }

}