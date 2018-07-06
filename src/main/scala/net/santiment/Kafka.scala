package net.santiment

import java.util.Properties

import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}


object Kafka extends LazyLogging {

  def consumer(config:KafkaConfig):KafkaConsumer[String,String] = {

    val props = new Properties()
    props.put("bootstrap.servers", config.bootstrapServers)
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("group.id", BuildInfo.name)
    props.put("client.id", BuildInfo.name)
    props.put("auto.offset.reset", "latest")

    //Skip messages from aborted transactions
    props.put("isolation.level", "read_committed")

    new KafkaConsumer[String, String](props)
  }

  def producer(config:KafkaConfig):KafkaProducer[String, String] = {

    val properties = new Properties()
    properties.put("bootstrap.servers", config.bootstrapServers)
    properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    // Wait until the value has been fully written to all kafka servers before acknowledgement
    properties.put("acks", "all")

    //Write compressed batches to kafka
    properties.put("compression.type", "lz4")

    //Batch size of 512 KB
    properties.put("batch.size", 1024 * 512)

    //Linger after a record is received in case more records are coming. In this way we can batch many records and send them at once
    properties.put("linger.ms", 1000)

    properties.put("client.id", BuildInfo.name)

    //This should be unique for each exporter instance which is running. However we plan to run only a single instance.
    properties.put("transactional.id", BuildInfo.name)

    lazy val client: KafkaProducer[String, String] = {
      val result = new KafkaProducer[String, String](properties)
      result.initTransactions()
      logger.info(s"Connected to Kafka at ${config.bootstrapServers}")
      result
    }

  }

}