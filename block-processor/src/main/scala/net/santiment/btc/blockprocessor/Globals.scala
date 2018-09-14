package net.santiment.btc.blockprocessor

import java.util.Properties
import java.util.concurrent.TimeUnit

import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.contrib.streaming.state.{PredefinedOptions, RocksDBStateBackend}
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema

import scala.util.hashing.MurmurHash3

object Globals
  extends LazyLogging {

  lazy val config = new Config()

  lazy val stateBackend: StateBackend = makeRocksDBStateBackend(config.flink.checkpointDataURI)

  def makeRocksDBStateBackend(checkpointURI: String): StateBackend = {
    val result = new RocksDBStateBackend(checkpointURI, true)
    result.setPredefinedOptions(PredefinedOptions.FLASH_SSD_OPTIMIZED)
    result
  }


  lazy val env: StreamExecutionEnvironment = setupStreamExecutionEnvironment(
    StreamExecutionEnvironment.getExecutionEnvironment,
    stateBackend,
    config.flink
  )

  def setupStreamExecutionEnvironment(
                                       env: StreamExecutionEnvironment,
                                       stateBackend: StateBackend,
                                       config: FlinkConfig
                                     ): StreamExecutionEnvironment = {
    env.setStateBackend(stateBackend)

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
    env.addSource(source).uid(uid)
  }

}