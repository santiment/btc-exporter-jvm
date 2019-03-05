package net.santiment

import com.typesafe.scalalogging.LazyLogging
import net.santiment.btc.rawexporter.BuildInfo
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.common.{KafkaException, TopicPartition}

import collection.JavaConverters._

class BitcoinKafkaProducer
(
  world : Globals
)
  extends LazyLogging
{

  var sinkCommitted = false

  def processBlock(height: Int): Unit = {
    val sink = world.sink
    sink.begin()
    logger.trace(s"current_block=$height, begin_transaction")
    val block = world.bitcoinClient.getRawBlock(height)
    sink.send(height.toString, block)
    logger.trace(s"sent")
    try {
      sink.commit()
    } catch {
      case e:KafkaException =>
        logger.error(s"Exception while committing block $height")
        sink.abort()
        throw e
      case e:Throwable =>
        logger.error(s"Unhandled exception while committing block $height")
        throw e
    }
  }

  def lastProcessedBlockHeight(): Int = {
    /**
      * Returns the last committed block height on the blocks topic
      */
    val partition = new TopicPartition(world.config.kafkaTopic,0)
    val partitionList = List(partition).asJava

    val consumer = world.consumer
    consumer.assign(List(partition).asJava)

    // Seek to the first commit after the last committed topic and then get the position
    // Get first position to read
    consumer.seekToBeginning(partitionList)
    val min_offset = consumer.position(partition)
    logger.debug(s"kafka-topic min offset: $min_offset")

    // Get last position to read
    consumer.seekToEnd(partitionList)
    val max_offset = consumer.position(partition)
    logger.debug(s"kafka-topic max offset: $max_offset")

    var offset = max_offset - 1
    var height = 0

    while(offset >= min_offset && height == 0) {
      logger.debug(s"testing offset $offset")
      consumer.seek(partition, offset)
      // Sometimes the following poll returns empty record, even though it should have returned something. When that
      // happens offset is decreased by 1. Due to this issue it might happen that the first consumed record in our loop
      // is not actually the last record in the topic.
      var result = consumer.poll(1000)
      logger.debug(s"new postion: ${consumer.position(partition)}")
      if(result.isEmpty) {
        offset-=1
      } else {
        // We already consumed a record. Now go forward in the topic until we have consumed the last record. After
        // consuming the last record the consumer offset should become equal to max_offset (assuming no one is writing
        // to the topic while this function is running)
        do {
          height = Math.max(height, result.records(partition).asScala.map(_.key().toInt).max)
          logger.debug(s"Found new max height: $height")
          result = consumer.poll(1000)
          logger.debug(s"new position: ${consumer.position(partition)}")
          // We assume that no one is writing to the topic while the seek is happening
        } while (consumer.position(partition) < max_offset)
      }
    }

    height
  }

  /**
   * Main function. Should be called from a cron job which runs once every 10 minutes
   */

  def main(args: Array[String]): Unit = {
    logger.info(s"Starting raw BTC exporter ${BuildInfo.version}")

    world.migrator.up()

    sys.addShutdownHook {
      logger.error("Attempting graceful shutdown")
      world.closeEverythingQuietly()
    }

    try {

      val lastWritten = lastProcessedBlockHeight()

      logger.info(s"last_written_block=$lastWritten")

      //Fetch blocks until present
      val lastToBeWritten = world.bitcoinClient.blockCount - world.config.confirmations
      logger.info(s"first_block_to_fetch=${lastWritten+1}, last_block_to_fetch=$lastToBeWritten")

      var startTs = System.currentTimeMillis()
      var blocks = 0
      for (height <- (lastWritten + 1) to lastToBeWritten) {
        logger.debug(s"current_block=$height")
        processBlock(height)
        blocks += 1
        var test = System.currentTimeMillis()
        if (test - startTs > 60000) {
          logger.info(s"blocks=$blocks, interval=${(test-startTs)/1000}s, last_block=$height")
          startTs = test
          blocks = 0
        }
      }

    } catch {
      case e:Exception =>
        logger.error("Unhandled exeption", e)
        world.closeEverythingQuietly()
        throw e
    }
  }
}

object App extends BitcoinKafkaProducer(Globals) {}
