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
  blockCacheSizeMb: Long,
  parallelism: Int,
  writeBufferSizeMb: Long,
  maxWriteBufferNumber: Int,
  minWriteBufferNumberToMerge: Int
) extends RocksDBProfile {}


class Config(args:Array[String]) {

  lazy val props: ParameterTool = ParameterTool.fromArgs(args)

  def getO(env:String, default:String=null): Option[String] = {
    val prop = env.toLowerCase.replace('_','.')
    Option(props.get(prop))
      .orElse(sys.env.get(env))
      .orElse(Option(default))
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

  lazy val migrations = MigrationConfig(
    connectionString = get("ZOOKEEPER_URL", "localhost:2181"),
    namespace = s"${BuildInfo.name}",
    nextMigrationPath = "/migration/next",
    nextMigrationToCleanPath = "/migration/nextToDestroy"
  )

  lazy val profile: RocksDBProfile = get("ROCKSDB_PROFILE","current") match {
    case "current" => CurrentJob
    case "historical" => HistoricalJob(
      get("ROCKSDB_BLOCK_CACHE_SIZE_MB","4096").toLong,
      get("ROCKSDB_PARALLELISM", "8").toInt,
      get("ROCKSDB_WRITE_BUFFER_SIZE_MB","256").toLong,
      get("ROCKSDB_MAX_WRITE_BUFFER_NUMBER", "5").toInt,
      get("ROCKSDB_MIN_WRITE_BUFFER_NUMBER_TO_MERGE","2").toInt
    )
    case x =>
      throw new IllegalArgumentException(s"Unknown profile: $x")
  }

}