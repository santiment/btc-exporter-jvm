package net.santiment

import java.net.URL
import java.nio.charset.StandardCharsets
import java.util.{Base64, Properties}

import com.fasterxml.jackson.databind.ObjectMapper
import com.googlecode.jsonrpc4j.JsonRpcHttpClient
import com.typesafe.scalalogging.LazyLogging
import net.santiment.btc.rawexporter.BuildInfo
import net.santiment.util.{MigrationUtil, Migrator, Store, ZookeeperStore}
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer

import collection.JavaConverters._
import net.santiment.util.Store.IntSerde
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.common.TopicPartition

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
    namespace = s"${BuildInfo.name}"
  )

  lazy val kafkaTopic = "btc-raw-blocks"

  lazy val confirmations: Int = sys.env.getOrElse("CONFIRMATIONS","3").toInt

  //5 minutes sleep after block is processed. (Bitcoin makes one block each 10 minutes so this should be even larger)
  lazy val sleepBetweenRunsMs = 300000

  lazy val zkNextMigrationPath = "/migration/next"
  lazy val zkNextMigrationToCleanPath = "/migration/nextToDestroy"

}

/**
  * This class wires the whole program together.
  */
class Globals extends LazyLogging
{

  logger.debug("Initialising globals")

  lazy val config: Config = new Config()

  lazy val zk:CuratorFramework = makeZookeeperClient(config.zk)

  lazy val adminClient: AdminClient = makeKafkaAdminClient(config.kafka)

  //Migration store data
  lazy val nextMigrationStore: Store[Int] = new ZookeeperStore[Int](zk, config.zkNextMigrationPath)
  lazy val nextMigrationToCleanStore: Store[Int] = new ZookeeperStore[Int](zk, config.zkNextMigrationToCleanPath)

  lazy val migrations = Array(
    MigrationUtil.topicMigration(adminClient, "btc-raw-blocks",1,1)
  )

  lazy val migrator = new Migrator(migrations, nextMigrationStore, nextMigrationToCleanStore)

  lazy val producer:KafkaProducer[String, Array[Byte]] = makeKafkaProducer(config.kafka)

  lazy val consumer:KafkaConsumer[String, Array[Byte]] = makeKafkaConsumer(config.kafka)
  lazy val sink:TransactionalSink[Array[Byte]] = new KafkaByteArraySink(producer, config.kafkaTopic)

  lazy val bitcoindJsonRpcClient: JsonRpcHttpClient = makeBitcoindJsonRpcClient(config.bitcoind)
  lazy val bitcoindBatchJsonRpcClient: BatchJsonRPCClient = makeBitcoindBatchJsonRpcClient(config.bitcoind)
  lazy val bitcoinClient:BitcoinClient = new BitcoinClient(bitcoindJsonRpcClient, bitcoindBatchJsonRpcClient)

  def makeBitcoindJsonRpcClient(config:BitcoinClientConfig): JsonRpcHttpClient = {
    logger.debug("Creating jsonrpc client")
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

  def makeBitcoindBatchJsonRpcClient(config:BitcoinClientConfig): BatchJsonRPCClient = {
    logger.debug("Creating batch jsonrpc client")
    val url:URL = new URL(s"http://${config.host}:${config.port}")
    val authString = s"${config.username}:${config.password}"

    val encodedAuthString: String = Base64
      .getEncoder
      .encodeToString(
        authString.getBytes(StandardCharsets.UTF_8))

    val headers: Map[String, String] = Map[String,String](("Authorization",s"Basic $encodedAuthString"))

    val mapper = new ObjectMapper()

    new BatchJsonRPCClient(mapper, url, headers)

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

  def makeKafkaConsumer(config: KafkaConfig): KafkaConsumer[String, Array[Byte]] = {

    val properties = new Properties()
    properties.put("bootstrap.servers", config.bootstrapServers)
    properties.put("group.id", "")
    properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer")

    properties.setProperty("bootstrap.servers", config.bootstrapServers)

    properties.put("enable.auto.commit", "false")
    properties.put("auto.offset.reset", "earliest")
    properties.put("isolation.level", "read_committed")

    val client: KafkaConsumer[String, Array[Byte]] = new KafkaConsumer[String, Array[Byte]](properties)
    logger.info(s"Consumer connected to Kafka at ${config.bootstrapServers}")
    client
  }


  def makeKafkaAdminClient(config:KafkaConfig):AdminClient = {
    val properties = new Properties()
    properties.put("bootstrap.servers", config.bootstrapServers)
    AdminClient.create(properties)
  }

  /**
    * Attempt to close gracefully all connections
    */
  def closeEverythingQuietly(): Unit = {
      producer.close()
  }


}

object Globals extends Globals {}
