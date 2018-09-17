package net.santiment.btc.blockprocessor

import java.time.Duration

import net.santiment.BuildInfo

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


class Config {

  val flink = FlinkConfig(
    checkpointDataURI = sys.env("CHECKPOINT_DATA_URI"),

    checkpointInterval = Duration.ofMillis(sys.env.getOrElse("CHECKPOINT_INTERVAL_MS", "1800000").toLong),

    checkpointTimeout = Duration.ofMillis(sys.env.getOrElse("CHECKPOINT_TIMEOUT_MS", "3600000").toLong),

    minPauseBetweenCheckpoints = Duration.ofMillis(sys.env.getOrElse("MIN_PAUSE_BETWEEN_CHECKPOINTS",   "600000").toLong),

    externalizedCheckpointsEnabled = true,

    maxNumberOfRestartsInInterval = sys.env.get("MAX_NUMBER_OF_RESTARTS_IN_INTERVAL")
        .map(_.toInt)
        .getOrElse(5),

    restartsInterval = Duration.ofSeconds(
      sys.env.getOrElse("RESTARTS_INTERVAL_S","3600").toInt),

    delayBetweenRestarts = Duration.ofSeconds(
      sys.env.getOrElse("DELAY_BETWEEN_RESTARTS_S","300").toInt)
  )

  lazy val rawBlockTopic = KafkaTopicConfig(
    //We support different Kafka clusters for the input and output topics in this way
    sys.env.getOrElse("KAFKA_RAW_BLOCK_URL", sys.env("KAFKA_URL")),
    sys.env.getOrElse("KAFKA_RAW_BLOCK_TOPIC", "btc-raw-blocks")
  )

  lazy val transfersTopic = KafkaTopicConfig(
    sys.env.getOrElse("KAFKA_TRANSFERS_URL", sys.env.getOrElse("KAFKA_URL","localhost:9092")),
    sys.env.getOrElse("KAFKA_TRANSFERS_TOPIC", "btc-transfers")
  )

  lazy val migrations = MigrationConfig(
    connectionString = sys.env.getOrElse("ZOOKEEPER_URL", "localhost:2181"),
    namespace = s"${BuildInfo.name}",
    nextMigrationPath = "/migration/next",
    nextMigrationToCleanPath = "/migration/nextToDestroy"
  )

}