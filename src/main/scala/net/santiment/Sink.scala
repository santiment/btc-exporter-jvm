package net.santiment

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

trait Sink[T] {
  def send(value:T)
  def flush()
}

trait TransactionalSink[T] extends Sink[T] with Transactional

class KafkaSink[T](producer:KafkaProducer[String, Array[Byte]], topic:String) extends TransactionalSink[T] {

  private val mapper = {
    val result = new ObjectMapper()
    result.registerModule(DefaultScalaModule)
  }

  override def begin(): Unit = producer.beginTransaction()
  override def commit(): Unit = producer.commitTransaction()
  override def abort(): Unit = producer.abortTransaction()

  override def send(value:T): Unit = {
    val bytes = mapper.writeValueAsBytes(value)
    val record = new ProducerRecord[String,Array[Byte]](topic,bytes)
    producer.send(record)
  }

  override def flush(): Unit = {
    producer.flush()
  }
}