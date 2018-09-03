package net.santiment

import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.common.KafkaException

class BitcoinKafkaProducer
(
  world : {
    val config: Config
    val bitcoinClient: BitcoinClient
    val lastBlockStore: TransactionalStore[Int]
    val sink: TransactionalSink[ByteArray]
    def closeEverythingQuietly (): Unit
    val migrator: Migrator
  }
)
  extends LazyLogging
{

  var sinkCommitted = false

  def processBlock(height: Int): Unit = {
    logger.debug(s"current_block=$height, begin_process")
    val block = world.bitcoinClient.getRawBlock(height)

      world.sink.send(height.toString, block)
      logger.trace(s"sent")
  }

  def makeTxProcessor[T](lastBlock:TransactionalStore[Int], sink:TransactionalSink[T], processor: Int =>Unit)(height:Int): Unit = {
    //Start a new transaction
    sink.begin()
    logger.trace(s"current_block=$height, begin_transaction")
    lastBlock.begin()

    //Process the current block and send the resulting records to Kafka
    processor(height)
    sink.flush()

    /**
      * Here starts the critical section for committing the data. First we record that all records have been sent to kafka, by updating
      * the last written block. From this moment on until the end, the last written and the last committed block differ. If anything happens
      * and the program crashes unexpectedly we will know it and will refuse to restart next time, since the state might have become inconsistent.
      * This algorithm guarantees that we won't get duplicated records in Kafka - either everything will work fine, or the program will crash in a
      * way which requires human intervention.
      */

    //Update is better than write, since writes do reads to see if the object exists in Zookeeper
    lastBlock.update(height)
    try {
      sinkCommitted = false
      sink.commit()
      sinkCommitted = true
      // If the process gets terminated here we will try to commit the last block to Zookeeper during shutdown
      lastBlock.commit()
      sinkCommitted = false
    } catch {
      case e:KafkaException =>
        logger.error(s"Exception while committing block $height")
        sink.abort()

        //Revert the last written block to its previous value. After restarting the client we will be able to continue normally
        lastBlock.abort()
        throw e
      case e:Throwable =>
        if(!sinkCommitted) {
          lastBlock.abort()
        }
        logger.error(s"Unhandled exception while committing block $height")
        throw e
    }
  }

  lazy val txProcess: Int => Unit = makeTxProcessor(world.lastBlockStore, world.sink, processBlock)

  /**
   * Main function. Should be called from a cron job which runs once every 10 minutes
   */

  def main(args: Array[String]): Unit = {
    logger.info(s"Starting raw BTC exporter ${BuildInfo.version}")

    world.migrator.up()

    sys.addShutdownHook {
      logger.error("Attempting graceful shutdown")
      if(sinkCommitted) {
        world.lastBlockStore.abort()
      }
      world.closeEverythingQuietly()
    }

    try {
      // Get last written block or 0 if none exist yet
      if (world.lastBlockStore.read.isEmpty) {
        //Since this operation is not a part of a transaction it will update both the write and commit store
        world.lastBlockStore.create(0)
      }

      val lastWritten: Int = world.lastBlockStore.read
        .map(_.intValue).get

      logger.info(s"last_written_block=$lastWritten")

      //Fetch blocks until present
      val lastToBeWritten = world.bitcoinClient.blockCount - world.config.confirmations
      logger.info(s"first_block_to_fetch=${lastWritten+1}, last_block_to_fetch=$lastToBeWritten")

      var startTs = System.currentTimeMillis()
      var blocks = 0
      for (height <- (lastWritten + 1) to lastToBeWritten) {
        logger.debug(s"current_block=$height")
        txProcess(height)
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
