package net.santiment

import java.net.URL
import java.nio.charset.StandardCharsets
import java.util.{Base64, Properties}

import com.fasterxml.jackson.databind.ObjectMapper
import com.googlecode.jsonrpc4j.JsonRpcHttpClient
import com.typesafe.scalalogging.LazyLogging
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import collection.JavaConverters._

/**
  * Configuration class with given default values. If you want to change some of those for testing, extend the class anc override the necessary settings
  */
class Config {

  lazy val bitcoind = BitcoinClientConfig(
    host = sys.env.getOrElse("BITCOIND_URL", "localhost"),
    port = sys.env.getOrElse("BITCOIND_PORT", "8332"),
    username = sys.env("BITCOIND_USER"),
    password = sys.env("BITCOIND_PASSWORD")
  )

  lazy val kafka = KafkaConfig(
    bootstrapServers = sys.env("KAFKA_URL")
  )

  lazy val zk = ZookeeperConfig(
    connectionString = sys.env.getOrElse("ZOOKEEPER_URL", "localhost:2181"),
    namespace = s"${BuildInfo.name}",
  )

  lazy val kafkaTopic = sys.env("KAFKA_TOPIC")

  lazy val zkLastWrittenPath = s"/$kafkaTopic/last-writen-block-height"
  lazy val zkLastComittedPath = s"/$kafkaTopic/last-commited-block-height"

  lazy val confirmations: Int = sys.env.getOrElse("CONFIRMATIONS","3").toInt

  //5 minutes sleep after block is processed. (Bitcoin makes one block each 10 minutes so this should be even larger)
  lazy val sleepBetweenRunsMs = 300000
}

/**
  * This class wires the whole program together.
  */
class Globals extends LazyLogging
{

  lazy val config: Config = new Config()

  lazy val zk:CuratorFramework = makeZookeeperClient(config.zk)

  lazy val lastWrittenHeightStore: Store[Integer] = new ZookeeperStore[Integer](zk, config.zkLastWrittenPath)
  lazy val lastCommittedHeightStore: Store[Integer] = new ZookeeperStore[Integer](zk, config.zkLastComittedPath)
  lazy val lastBlockStore: TransactionalStore[Integer] = new SimpleTxStore[Integer](lastWrittenHeightStore, lastCommittedHeightStore)

  lazy val producer:KafkaProducer[String, Array[Byte]] = makeKafkaProducer(config.kafka)
  lazy val sink:TransactionalSink[ResultTx] = new KafkaSink[ResultTx](producer, config.kafkaTopic)

  lazy val bitcoindJsonRpcClient: JsonRpcHttpClient = makeBitcoindJsonRpcClient(config.bitcoind)

  lazy val bitcoin:BitcoinClient = new BitcoinClient(bitcoindJsonRpcClient)

  def makeBitcoindJsonRpcClient(config:BitcoinClientConfig): JsonRpcHttpClient = {

    val url:URL = new URL(s"http://${config.host}:${config.port}")
    val authString = s"${config.username}:${config.password}"

    val encodedAuthString: String = Base64
      .getEncoder
      .encodeToString(
        authString.getBytes(StandardCharsets.UTF_8))

    val headers: Map[String, String] = Map[String,String](("Authorization",s"Basic $encodedAuthString"))

    val mapper = new ObjectMapper()

    new JsonRpcHttpClient(mapper, url, headers.asJava)

  }

  def makeZookeeperClient(config:ZookeeperConfig):CuratorFramework = {
    logger.debug(s"Building Zookeeper client")

    val retryPolicy = new ExponentialBackoffRetry(1000, 10)

    val client = CuratorFrameworkFactory.builder()
      .namespace(config.namespace)
      .connectString(config.connectionString)
      .retryPolicy(retryPolicy)
      .build()

    logger.debug(s"Connecting to Zookeeper at ${config.connectionString}")
    client.start()
    logger.debug(s"Blocking until connected")
    client.blockUntilConnected()
    logger.info(s"Connected to Zookeeper at ${config.connectionString}. Namespace: ${config.namespace}")
    client
  }

  def makeKafkaConsumer(config:KafkaConfig): KafkaConsumer[String, String] = {
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

  def makeKafkaProducer(config:KafkaConfig):KafkaProducer[String, Array[Byte]] = {

    val properties = new Properties()
    properties.put("bootstrap.servers", config.bootstrapServers)
    properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")


    // Wait until the value has been fully written to all kafka servers before acknowledgement
    properties.put("acks", "all")

    //Write compressed batches to kafka
    properties.put("compression.type", "lz4")

    //Batch size of 512 KB
    properties.put("batch.size", (1024 * 512).toString)

    //Linger after a record is received in case more records are coming. In this way we can batch many records and send them at once
    properties.put("linger.ms", 1000.toString)

    properties.put("client.id", BuildInfo.name)

    //This should be unique for each exporter instance which is running. However we plan to run only a single instance.
    properties.put("transactional.id", BuildInfo.name)

    val client: KafkaProducer[String, Array[Byte]] = new KafkaProducer[String, Array[Byte]](properties)
    client.initTransactions()
    logger.info(s"Connected to Kafka at ${config.bootstrapServers}")

    client
  }

  /**
    * Attempt to close gracefully all connections
    */
  def closeEverythingQuietly(): Unit = {
    try {
      zk.close()
    } finally {
      producer.close()
    }
  }


}

object Globals extends Globals {}
