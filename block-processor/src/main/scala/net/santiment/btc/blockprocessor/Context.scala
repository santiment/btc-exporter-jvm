package net.santiment.btc.blockprocessor

import java.nio.charset.StandardCharsets
import java.util.Properties
import java.util.concurrent.TimeUnit

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.typesafe.scalalogging.LazyLogging
import net.santiment.util.Store._
import net.santiment.util.{MigrationUtil, Migrator, Store, ZookeeperStore}
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.contrib.streaming.state.{OptionsFactory, PredefinedOptions, RocksDBStateBackend}
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011.Semantic
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}
import org.apache.flink.streaming.util.serialization.{KeyedDeserializationSchema, KeyedSerializationSchema}
import org.apache.kafka.clients.admin.AdminClient
import org.rocksdb.{BlockBasedTableConfig, BloomFilter, ColumnFamilyOptions, DBOptions}

import scala.util.hashing.MurmurHash3

class Context(args:Array[String])
  extends LazyLogging {

  lazy val config = new Config(args)

  //Migration stuff
  lazy val zk:CuratorFramework = makeZookeeperClient(config.migrations)

  def makeZookeeperClient(config:MigrationConfig):CuratorFramework = {
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


  lazy val transfersAdminClient: AdminClient = makeKafkaAdminClient(config.transfersTopic)

  def makeKafkaAdminClient(config:KafkaTopicConfig):AdminClient = {
    val properties = new Properties()
    properties.put("bootstrap.servers", config.bootstrapServers)
    AdminClient.create(properties)
  }

  //Migration store data
  lazy val nextMigrationStore: Store[Int] = new ZookeeperStore[Int](zk, config.migrations.nextMigrationPath)
  lazy val nextMigrationToCleanStore: Store[Int] = new ZookeeperStore[Int](zk, config.migrations.nextMigrationToCleanPath)

  lazy val migrations = Array(
    MigrationUtil.compactTopicMigration(transfersAdminClient, config.transfersTopic.topic,1,1)
  )

  lazy val migrator = new Migrator(migrations, nextMigrationStore, nextMigrationToCleanStore)


  lazy val stateBackend: StateBackend = makeRocksDBStateBackend(config.flink.checkpointDataURI, config.profile)

  def makeRocksDBStateBackend(checkpointURI: String, profile:RocksDBProfile): StateBackend = {
    val result = new RocksDBStateBackend(checkpointURI, true)

    profile match {
      case CurrentJob =>
        result.setPredefinedOptions(PredefinedOptions.FLASH_SSD_OPTIMIZED)

      case conf:HistoricalJob =>
        //We need a lot of cache to process the records fast
        result.setOptions( new OptionsFactory {
          override def createDBOptions(currentOptions: DBOptions): DBOptions = {
            //Those are the predefined FLASH_SSD_OPTIMIZED options
            new DBOptions()
              .setIncreaseParallelism(conf.parallelism)
              .setUseFsync(false)
              .setMaxOpenFiles(-1)


          }

          override def createColumnOptions(currentOptions: ColumnFamilyOptions): ColumnFamilyOptions = {
            new ColumnFamilyOptions()
              .optimizeForPointLookup(conf.blockCacheSizeMb)
              .setMaxWriteBufferNumber(conf.maxWriteBufferNumber) //default 2
              .setMinWriteBufferNumberToMerge(conf.minWriteBufferNumberToMerge) //default 1
              .setOptimizeFiltersForHits(true)
              .setWriteBufferSize(conf.writeBufferSizeMb * 1024 * 1024) //256MB, default is 4MB
              .setTableFormatConfig(
              new BlockBasedTableConfig()
                .setBlockCacheSize(conf.blockCacheSizeMb*1024*1024)
                .setFilter( new BloomFilter()) //bloom filters are apparently needed for reducing reads
            )
          }
        })
    }

    result
  }


  lazy val env: StreamExecutionEnvironment = setupStreamExecutionEnvironment(
    StreamExecutionEnvironment.getExecutionEnvironment,
    stateBackend,
    config.flink,
    config.props
  )

  def setupStreamExecutionEnvironment(
                                       env: StreamExecutionEnvironment,
                                       stateBackend: StateBackend,
                                       config: FlinkConfig,
                                       props: ExecutionConfig.GlobalJobParameters
                                     ): StreamExecutionEnvironment = {
    env.setStateBackend(stateBackend)

    //Expose arguments to web ui
    env.getConfig.setGlobalJobParameters(props)

    // Checkpoint config
    env.enableCheckpointing(config.checkpointInterval.toMillis)
    env.getCheckpointConfig.setCheckpointTimeout(config.checkpointTimeout.toMillis)
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(config.minPauseBetweenCheckpoints.toMillis)

    env.getJavaEnv match {
      case _: LocalStreamEnvironment =>
        // Don't externalize when running locally
        logger.info("Checkpoint externalization disabled.")
      case _ =>
        logger.info("Checkpoint externalization enabled.")
        if (config.externalizedCheckpointsEnabled)
          env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    }

    // All intervals in restart strategy are set in seconds.
    env.setRestartStrategy(RestartStrategies.failureRateRestart(
      config.maxNumberOfRestartsInInterval,
      Time.of(config.restartsInterval.toMillis, TimeUnit.MILLISECONDS),
      Time.of(config.delayBetweenRestarts.toMillis, TimeUnit.MILLISECONDS)
    ))

    // Set time to be event time.
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    env
  }

  lazy val rawBlockSource: DataStream[RawBlock] = makeRawBlockSource(env, config.rawBlockTopic)

  def makeRawBlockSource(env: StreamExecutionEnvironment, config: KafkaTopicConfig): DataStream[RawBlock] = {
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", config.bootstrapServers)


    // we rely solely on flink checkpointing and ignore kafka offset committing and group id
    properties.setProperty("group.id", "")
    properties.setProperty("enable.auto.commit", "false")
    properties.setProperty("auto.offset.reset", "earliest")

    //We use transactions
    properties.setProperty("isolation.level", "read_committed")


    val deserializationSchema: KeyedDeserializationSchema[RawBlock] = new KeyedDeserializationSchema[RawBlock] {
      override def deserialize(messageKey: Array[Byte], message: Array[Byte], topic: String, partition: Int, offset: Long): RawBlock = {
        RawBlock(new String(messageKey).toInt, message.clone())
      }

      override def isEndOfStream(nextElement: RawBlock): Boolean = false

      override def getProducedType: TypeInformation[RawBlock] = implicitly[TypeInformation[RawBlock]]
    }

    val source = new FlinkKafkaConsumer011(config.topic, deserializationSchema, properties)

    // TODO: explore how we can use setStartFromTimestamp(...) so we can start from an arbitrary time (the producer
    // should add the timestamps when it fills the topic)

    // start from the earliest record possible every time
    source.setStartFromEarliest()
    // we rely solely on flink checkpointing and ignore kafka offset committing and group id
    source.setCommitOffsetsOnCheckpoints(false)

    // We create a uid based on the name of the kafka topic. In this way if we change the topic processing
    // will restart by itself from the beginning even if the job is started from a savepoint

    val uid = s"raw-blocks-kafka-${MurmurHash3.stringHash(config.topic).toHexString}"
    env.addSource(source).uid(uid).setParallelism(1)
  }

  lazy val consumeTransfers:DataStream[AccountChange]=>Unit = makeTransfersKafkaSink(config.transfersTopic)


  def makeTransfersKafkaSink(config: KafkaTopicConfig): DataStream[AccountChange]=>Unit
  = {
    logger.info(s"Connecting transfers sink to ${config.bootstrapServers}, topic: ${config.topic}")
    val properties = new Properties()

    properties.setProperty("bootstrap.servers", config.bootstrapServers)
    properties.setProperty("acks", "all")

    //Write compressed batches to kafka
    properties.put("compression.type", "lz4")


    val serializationSchema: KeyedSerializationSchema[AccountChange] = new KeyedSerializationSchema[AccountChange] {

      lazy val objectMapper: ObjectMapper = {
        val result = new ObjectMapper()
        result.registerModule(DefaultScalaModule)
        result
      }

      override def serializeKey(element: AccountChange): Array[Byte] = {
        //Make a unique key for each record so that we can compact the topic
        s"${element.height}-${element.txPos}-${if(element.in) "in" else "out"}-${element.index}".getBytes(StandardCharsets.UTF_8)
      }

      override def serializeValue(element: AccountChange): Array[Byte] = {
        objectMapper.writeValueAsBytes(element)
      }

      override def getTargetTopic(element: AccountChange): String = {
        config.topic
      }
    }

    val producer = new FlinkKafkaProducer011[AccountChange](config.topic, serializationSchema,properties, Semantic.AT_LEAST_ONCE)
    producer.setWriteTimestampToKafka(true)

    // We create a uid based on the name of the kafka topic. In this way if we change the topic any old saved state will
    // not affect the new processing
    val uid = s"btc-transfers-kafka-${MurmurHash3.stringHash(config.topic).toHexString}"

    stream=>stream.addSink(producer).uid(uid)

  }

}