package net.santiment

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import java.nio.ByteBuffer
import java.util.{Timer, TimerTask}

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

case class ZookeeperStats
(
  var getData: Long = 0L,
  var getDataTime: Long = 0L,
  var setData: Long = 0L,
  var setDataTime: Long = 0L,
  var create: Long = 0L,
  var createTime: Long = 0L,
  var delete: Long = 0L,
  var deleteTime: Long = 0L
) {
  def minus(other: ZookeeperStats): ZookeeperStats = ZookeeperStats(
    getData - other.getData,
    getDataTime - other.getDataTime,
    setData - other.setData,
    setDataTime - other.setDataTime,
    create - other.create,
    createTime - other.createTime,
    delete - other.delete,
    deleteTime - other.deleteTime
  )

  override def toString: String = {
    val totalTime = getDataTime + setDataTime + createTime + deleteTime
    s"ZookeeperStats(getData=$getData, getDataTime=$getDataTime, setData=$setData, setDataTime=$setDataTime, create=$create, createTime=$createTime, delete=$delete, deleteTime=$deleteTime, totalTime=$totalTime)"
  }
}

object ZookeeperStats extends LazyLogging {
  val stats = ZookeeperStats()

  val timer = new Timer("ZookeeperStats", true)

  val task: TimerTask = new TimerTask {

    var oldStats = ZookeeperStats()
    override def run(): Unit = {
      val diff = stats.minus(oldStats)
      logger.info(s"Zookeeper stats: $diff")
      oldStats = stats.copy()
    }
  }

  timer.scheduleAtFixedRate(task,60000,60000)
}

class ZookeeperStore[T](private val client:CuratorFramework, path:String)(implicit serde:Serde[T])
  extends Store[T]
    with LazyLogging {

  import ZookeeperStats.stats

  override def read: Option[T] = {

    val start = System.nanoTime()

    //To optimize the normal path we first read and then check if the element exists
    val result = try {
      val bytes = client.getData.forPath(path)
      Some(serde.fromByteArray(bytes))
    } catch {
      case e:Exception =>
        if(client.checkExists().forPath(path) == null) {
          None
        } else {
          throw e
        }
    }

    stats.getData += 1
    stats.getDataTime += (System.nanoTime() - start)

    result
  }

  override def update(value:T): Unit = {
    val start = System.nanoTime()

    client.setData().forPath(path, serde.toByteArray(value))

    stats.setData += 1
    stats.setDataTime += (System.nanoTime() - start)
  }

  override def create(value:T): Unit = {
    val start = System.nanoTime()

    client.create().creatingParentsIfNeeded().forPath(path, serde.toByteArray(value))

    stats.create += 1
    stats.createTime += (System.nanoTime() - start)
  }

  override def delete(): Unit = {
    val start = System.nanoTime()

    client.delete().deletingChildrenIfNeeded().forPath(path)

    stats.delete += 1
    stats.deleteTime += (System.nanoTime() - start)
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
