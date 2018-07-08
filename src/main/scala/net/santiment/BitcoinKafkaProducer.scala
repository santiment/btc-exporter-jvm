package net.santiment

import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.common.KafkaException

class BitcoinKafkaProducer
(
  world : {
    val config: Config
    val bitcoin: BitcoinClient
    val lastBlockStore: TransactionalStore[Integer]
    val sink: TransactionalSink
    def closeEverythingQuietly (): Unit
  }
)
  extends LazyLogging
{

  def processBlock(height: Int): Unit = {
    val block = world.bitcoin.getBlock(height)
  }

  def makeTxProcessor(lastBlock:TransactionalStore[Integer], sink:TransactionalSink, processor:Int=>Unit)(height:Int): Unit = {
    //Start a new transaction
    sink.begin()
    lastBlock.begin()

    //Process the current block and send the resulting records to Kafka
    processor(height)

    /**
      * Here starts the critical section for committing the data. First we record that all records have been sent to kafka, by updating
      * the last written block. From this moment on until the end, the last written and the last committed block differ. If anything happens
      * and the program crashes unexpectedly we will know it and will refuse to restart next time, since the state might have become inconsistent.
      * This algorithm guarantees that we won't get duplicated records in Kafka - either everything will work fine, or the program will crash in a
      * way which requires human intervention.
      */

    lastBlock.write(Some(height))
    try {
      sink.commit()
    } catch {
      case e:KafkaException =>
        logger.error(s"Exception while committing block $height")
        sink.abort()

        //Revert the last written block to its previous value. After restarting the client we will be able to continue normally
        lastBlock.abort()
        throw e
      case e:Throwable =>
        logger.error(s"Unhandled exception while committing block $height")
        throw e
    }

    //Inform zk that the transaction has been committed successfully. Here the critical section ends
    lastBlock.commit()

  }

  lazy val txProcess: Int => Unit = makeTxProcessor(world.lastBlockStore, world.sink, processBlock)

  /**
   * Main loop
   */

  def main(args: Array[String]): Unit = {
    try {
      // Get last written block or 0 if none exist yet
      val lastWritten: Int = world.lastBlockStore.read
        .map(_.intValue)
        .getOrElse(0)

      while (true) {
        //Fetch blocks
        val lastToBeWritten = world.bitcoin.blockCount - world.config.confirmations

        for (height <- (lastWritten + 1) to lastToBeWritten) {
          txProcess(height)
        }
        Thread.sleep(world.config.sleepBetweenRunsMs)
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