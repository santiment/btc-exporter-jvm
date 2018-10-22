package net.santiment.btc.blockprocessor

import java.time.Duration

import org.apache.flink.api.java.utils.ParameterTool

case class KafkaTopicConfig
(
  bootstrapServers: String,
  topic: String
)

case class MigrationConfig
(
  connectionString: String,
  namespace: String,
  nextMigrationPath: String,
  nextMigrationToCleanPath: String
)


case class FlinkConfig
(
  checkpointDataURI: String,
  checkpointInterval: Duration,
  checkpointTimeout: Duration,
  minPauseBetweenCheckpoints: Duration,
  externalizedCheckpointsEnabled: Boolean,
  maxNumberOfRestartsInInterval: Int,
  restartsInterval: Duration,
  delayBetweenRestarts: Duration
)

sealed trait RocksDBProfile
case object CurrentJob extends RocksDBProfile {}
case class HistoricalJob
(
  blockSize: Long,
  blockCacheSizeMb: Long,
  parallelism: Int,
  writeBufferSizeMb: Long,
  maxWriteBufferNumber: Int,
  minWriteBufferNumberToMerge: Int
) extends RocksDBProfile {}

case class Features
(
  transfers: Boolean,
  stackChanges: Boolean
)


class Config(args:Array[String]) {


  lazy val props: ParameterTool = ParameterTool.fromArgs(args)

  def getO(env:String, default:String=null): Option[String] = {
    val prop = env.toLowerCase.replace('_','.')
    if(props.has(prop)) {
      Option(props.get(prop))
    } else {
      sys.env.get(env).orElse(Option(default)).map {dflt=> props.get(prop,dflt)}
    }
  }

  def get(env:String, default:String = null):String = getO(env,default).get

  lazy val flink = FlinkConfig(

    checkpointDataURI = get("CHECKPOINT_DATA_URI"),

    checkpointInterval = Duration.ofMillis(get("CHECKPOINT_INTERVAL_MS","1800000").toLong),

    checkpointTimeout = Duration.ofMillis(get("CHECKPOINT_TIMEOUT_MS", "3600000").toLong),

    minPauseBetweenCheckpoints = Duration.ofMillis(get("MIN_PAUSE_BETWEEN_CHECKPOINTS",   "600000").toLong),

    externalizedCheckpointsEnabled = true,

    maxNumberOfRestartsInInterval = get("MAX_NUMBER_OF_RESTARTS_IN_INTERVAL","5").toInt,

    restartsInterval = Duration.ofSeconds(
      get("RESTARTS_INTERVAL_S","3600").toInt),

    delayBetweenRestarts = Duration.ofSeconds(
      get("DELAY_BETWEEN_RESTARTS_S","300").toInt)
  )

  lazy val rawBlockTopic = KafkaTopicConfig(
    //We support different Kafka clusters for the input and output topics in this way
    getO("KAFKA_RAW_BLOCK_URL").orElse(getO("KAFKA_URL","localhost:9092")).get,
    get("KAFKA_RAW_BLOCK_TOPIC", "btc-raw-blocks")
  )

  lazy val transfersTopic = KafkaTopicConfig(
    getO("KAFKA_TRANSFERS_URL").orElse(getO("KAFKA_URL","localhost:9092")).get,
    get("KAFKA_TRANSFERS_TOPIC", "btc-transfers")
  )

  lazy val stacksTopic = KafkaTopicConfig(
    getO("KAFKA_STACKS_URL").orElse(getO("KAFKA_URL", "localhost:9092")).get,
    get("KAFKA_STACKS_TOPIC", "btc-stacks")
  )

  lazy val migrations = MigrationConfig(
    connectionString = get("ZOOKEEPER_URL", "localhost:2181"),
    namespace = s"${BuildInfo.name}",
    nextMigrationPath = "/migration/next",
    nextMigrationToCleanPath = "/migration/nextToDestroy"
  )

  lazy val profile: RocksDBProfile = get("ROCKSDB_PROFILE","current") match {
    case "current" => CurrentJob
    case "historical" => HistoricalJob(
      blockSize = get("ROCKSDB_BLOCK_SIZE", "32768").toLong,
      blockCacheSizeMb = get("ROCKSDB_BLOCK_CACHE_SIZE_MB", "256").toLong,
      parallelism = get("ROCKSDB_PARALLELISM", "8").toInt,
      writeBufferSizeMb = get("ROCKSDB_WRITE_BUFFER_SIZE_MB", "16").toLong,
      maxWriteBufferNumber = get("ROCKSDB_MAX_WRITE_BUFFER_NUMBER", "5").toInt,
      minWriteBufferNumberToMerge = get("ROCKSDB_MIN_WRITE_BUFFER_NUMBER_TO_MERGE", "4").toInt
    )
    case x =>
      throw new IllegalArgumentException(s"Unknown profile: $x")
  }

  lazy val features =  Features(
    transfers = get("FEATURE_TRANSFERS", "true").toBoolean,
    stackChanges = get("FEATURE_STACKS", default ="false").toBoolean
  )

}