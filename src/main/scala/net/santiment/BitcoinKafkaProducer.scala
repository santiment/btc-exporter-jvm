package net.santiment

import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.common.KafkaException
import org.bitcoinj.core.{Sha256Hash, Transaction}
import org.bitcoinj.core.TransactionInput.{ConnectMode, ConnectionResult}

import collection.JavaConverters._
import scala.collection.mutable

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

    val txs = block.getTransactions
    val inputHashes = mutable.HashSet[Sha256Hash]()

    // Gather all input transactions that need to be retrieved
    txs.asScala.foreach { tx =>

      //Ignore coinbase txs
      //TODO: We shouldn't ignore them
      if(tx.isCoinBase) {
        return
      }

      tx.getInputs.forEach { input =>
        val hash = input.getParentTransaction.getHash
        inputHashes.add(hash)
      }
    }

    //Get all parent transactions
    val parents:collection.Map[Sha256Hash,Transaction] = world.bitcoin.getTxList(inputHashes)

    txs.asScala.foreach { tx =>

      //Connect input to parents
      tx.getInputs.forEach { input =>
        val result = input.connect(parents.asJava,ConnectMode.DISCONNECT_ON_CONFLICT)
        if (result != ConnectionResult.SUCCESS) {
          logger.error(s"Cannot connect tx for input ${input.getSequenceNumber} from tx ${tx.getHashAsString}")
          throw new IllegalStateException()
        }
      }

      //
    }


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
   * Main function. Should be called from a cron job which runs once every 10 minutes
   */

  def main(args: Array[String]): Unit = {
    try {
      // Get last written block or 0 if none exist yet
      val lastWritten: Int = world.lastBlockStore.read
        .map(_.intValue)
        .getOrElse(0)

      //Fetch blocks until present
      val lastToBeWritten = world.bitcoin.blockCount - world.config.confirmations

      for (height <- (lastWritten + 1) to lastToBeWritten) {
        txProcess(height)
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
