package net.santiment

import org.apache.kafka.clients.producer.KafkaProducer

trait Sink {

}

trait TransactionalSink extends Sink with Transactional

class KafkaSink(producer:KafkaProducer[String, String]) extends TransactionalSink {
  override def begin(): Unit = producer.beginTransaction()
  override def commit(): Unit = producer.commitTransaction()
  override def abort(): Unit = producer.abortTransaction()
}