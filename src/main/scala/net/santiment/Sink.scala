package net.santiment

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

trait Sink[T] {
  def send(value:T)
  def flush()
}

trait TransactionalSink[T] extends Sink[T] with Transactional

case class KafkaStats
(
  var begin:Long = 0L,
  var beginTime:Long = 0l,
  var commit:Long = 0l,
  var commitTime:Long = 0l,
  var abort:Long = 0l,
  var abortTime:Long = 0l,
  var send:Long = 0l,
  var sendTime: Long = 0l,
  var flush:Long=0L,
  var flushTime: Long = 0L,
) {
  def minus(other:KafkaStats):KafkaStats = KafkaStats(
    begin - other.begin,
    beginTime - other.beginTime,
    commit - other.commit,
    commitTime - other.commitTime,
    abort - other.abort,
    abortTime - other.abortTime,
    send - other.send,
    sendTime - other.sendTime,
    flush - other.flush,
    flushTime - other.flushTime
  )
}


class KafkaSink[T](producer:KafkaProducer[String, Array[Byte]], topic:String)
  extends TransactionalSink[T]
    with Periodic[KafkaStats]
    with LazyLogging{

  val stats:KafkaStats = KafkaStats()

  override val period: Int = 60000

  private val mapper = {
    val result = new ObjectMapper()
    result.registerModule(DefaultScalaModule)
  }

  override def begin(): Unit = {
    val start = System.nanoTime()
    producer.beginTransaction()
    stats.begin += 1
    stats.beginTime += (System.nanoTime() - start)
  }
  override def commit(): Unit = {
    val start = System.nanoTime()
    producer.commitTransaction()
    stats.commit +=1
    stats.commitTime += (System.nanoTime() - start)

    occasionally( oldStats => {
      val diff = if (oldStats != null) {stats.minus(oldStats)} else stats
      logger.info(s"Kafka stats: $diff")
      stats
    })

  }
  override def abort(): Unit = {
    val start = System.nanoTime()
    producer.abortTransaction()
    stats.abort += 1
    stats.abortTime += (System.nanoTime() - start)
  }

  override def send(value:T): Unit = {
    val bytes = mapper.writeValueAsBytes(value)
    val record = new ProducerRecord[String,Array[Byte]](topic,bytes)
    val start = System.nanoTime()
    producer.send(record)
    stats.send += 1
    stats.sendTime += (System.nanoTime()- start)
  }

  override def flush(): Unit = {
    val start = System.nanoTime()
    producer.flush()
    stats.flush += 1
    stats.flushTime += (System.nanoTime() - start)
  }
}