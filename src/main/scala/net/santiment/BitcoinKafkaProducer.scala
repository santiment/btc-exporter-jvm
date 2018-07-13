package net.santiment

import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.common.KafkaException
import org.bitcoinj.core._
import org.bitcoinj.core.TransactionInput.{ConnectMode, ConnectionResult}

import collection.JavaConverters._
import scala.collection.mutable
import java.{util => j}

class BitcoinKafkaProducer
(
  world : {
    val config: Config
    val bitcoin: BitcoinClient
    val lastBlockStore: TransactionalStore[Integer]
    val sink: TransactionalSink[ResultTx]
    def closeEverythingQuietly (): Unit
  }
)
  extends LazyLogging
{

  def modelTransfers(debits: Seq[TransactionEntry],
                     credits: Seq[TransactionEntry])
                    : j.List[(BitcoinAddress, BitcoinAddress,Coin)] = {

    //1. In case there are entries with repeating addresses group them into one
    val amounts:mutable.HashMap[BitcoinAddress,Long] = mutable.HashMap()

    for (te <- debits) {
      val value= te.value.getValue
      val old = amounts.getOrElse(te.account,0L)
      amounts.put(te.account, old+value)
    }

    for (te <- credits) {
      val value= te.value.getValue
      val old = amounts.getOrElse(te.account,0L)
      amounts.put(te.account, old+value)
    }

    //2.Order debits and credits by amount and match them greedily
    val buffer = amounts.toBuffer.filter(_._2 != 0).sortBy((x:(BitcoinAddress,Long))=>x._2)
    logger.trace(s"buffer=${buffer.map(_.toString())}")
    var result = new j.LinkedList[(BitcoinAddress,BitcoinAddress,Coin)]()
    var start = 0
    var remainder = buffer.head._2
    var end = buffer.length
    var creditValue = 0L
    var debitValue = 0L
    var comp = 0L

    while(end-start > 1) {
      logger.trace(s"loop start=$start, end=$end, remainder=$remainder, result=${result.asScala}")
      var transferValue: Long = 0L

      if (remainder < 0L) {

        end -= 1
        debitValue = buffer(end)._2

        comp = remainder + debitValue

        if (comp > 0L) {
          transferValue = -remainder
          remainder = remainder + debitValue
        } else {
          transferValue = debitValue
          remainder = comp
        }
      } else {

        start += 1

        creditValue = buffer(start)._2

        comp = remainder + creditValue

        if (comp > 0) {
          transferValue = -creditValue
          remainder = comp
        } else {
          transferValue = remainder
          remainder = comp
        }
      }
      logger.trace(s"loop end start=$start, end=$end, remainder=$remainder, comp=$comp, tV=$transferValue")
      if (transferValue != 0L) {
        result.add( (buffer(start)._1,
          buffer(end)._1,
          Coin.valueOf(transferValue)))
      }
    }

    result
  }

  def processBlock(height: Int): Unit = {
    logger.debug(s"current_block=$height, begin_process")
    val block = world.bitcoin.getBlock(height)

    val txs: j.List[Transaction] = block.getTransactions
    val nonCoinbaseTxs = txs.subList(1,txs.size())
    val coinbase = txs.get(0)

    var pushedEvents = 0L
    var blockFees = 0L

    logger.trace(s"current_block=$height, fetched_txs=${txs.size()}")
    val inputHashes = mutable.HashSet[Sha256Hash]()

    // Gather all input transactions that need to be retrieved
    for(
      tx:Transaction <- nonCoinbaseTxs.asScala;
      input:TransactionInput <- tx.getInputs.asScala
    ) {
      val hash = input.getOutpoint.getHash
      inputHashes.add(hash)
    }


    //Get all parent transactions
    val parents:collection.Map[Sha256Hash,Transaction] = world.bitcoin.getTxList(inputHashes)
    logger.debug(s"Retrieved parent txs $parents")


    //Process all non-coinbase txs and calculate the fees
    for(tx:Transaction <- nonCoinbaseTxs.asScala) {

      //Get list of debits and credits
      var debits = for(output:TransactionOutput <- tx.getOutputs.asScala) yield {
        val account = BitcoinClient.extractAddress(output.getScriptPubKey)
        val value:Coin = output.getValue
        TransactionEntry(account,value)
      }


      //Connect input to parents and return input list
      val credits = for(input:TransactionInput <- tx.getInputs.asScala) yield {

        val result = input.connect(parents.asJava, ConnectMode.DISCONNECT_ON_CONFLICT)
        if (result != ConnectionResult.SUCCESS) {
          logger.error(s"Cannot connect tx for input from tx ${tx.getHashAsString}. Result is $result")
          throw new IllegalStateException()
        }

        val account = BitcoinClient.extractAddress(input.getConnectedOutput.getScriptPubKey)
        val value: Coin = input.getConnectedOutput.getValue

        //We store credits as negative values
        TransactionEntry(account, value.negate())
      }

      //Compute fees. The fee for each non-coinbase tx is equal to the difference between the debits and the credits
      val totalCredit = credits.map(_.value.getValue).sum //This value is negative
      val totalDebit = credits.map(_.value.getValue).sum
      val fee = 0-(totalDebit + totalCredit)
      if(fee < 0) {
        throw new IllegalStateException("credit < debit")
      }

      val feeDebit = TransactionEntry(BitcoinAddress(s"fee_${tx.getHashAsString}","virtual"),Coin.valueOf(fee))
      blockFees -= fee

      debits = debits :+ feeDebit

      for (entry <- modelTransfers(debits,credits).asScala) {
        world.sink.send(ResultTx(
          from = entry._1.address,
          to = entry._2.address,
          value = entry._3.toPlainString.toDouble,
          blockNumber = height,
          timestamp = block.getTimeSeconds,
          transactionHash = tx.getHashAsString
        ))
        pushedEvents += 1
      }
    }

    //Process coinbase transaction
    val cbDebits = for(output:TransactionOutput <- coinbase.getOutputs.asScala) yield {
      val account = BitcoinClient.extractAddress(output.getScriptPubKey)
      val value:Coin = output.getValue
      TransactionEntry(account,value)
    }

    val minerReward = cbDebits.map(_.value.getValue).sum
    val minted = blockFees - minerReward //credit, i.e. negative
    //Create two inputs -- fee and coinbase
    val cbCredits = mutable.Buffer[TransactionEntry](
      TransactionEntry(BitcoinAddress("fee", "virtual"), Coin.valueOf(blockFees)),
      TransactionEntry(BitcoinAddress("mint", "virtual"), Coin.valueOf(minted))
    )

    for (entry <- modelTransfers(cbDebits,cbCredits).asScala) {
      val value = ResultTx(
        from = entry._1.address,
        to = entry._2.address,
        value = entry._3.toPlainString.toDouble,
        blockNumber = height,
        timestamp = block.getTimeSeconds,
        transactionHash = coinbase.getHashAsString
      )
      logger.trace(s"result_tx=$value")

      world.sink.send(value)
      logger.trace(s"sent")
      pushedEvents += 1
    }

    logger.debug(s"block_transactions=${txs.size()}, produced_events=$pushedEvents")

  }

  def makeTxProcessor(lastBlock:TransactionalStore[Integer], sink:TransactionalSink[ResultTx], processor:Int=>Unit)(height:Int): Unit = {
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

    sys.addShutdownHook {
      world.closeEverythingQuietly()
      sys.exit(0)
    }

    try {
      // Get last written block or 0 if none exist yet
      val lastWritten: Int = world.lastBlockStore.read
        .map(_.intValue)
        .getOrElse(0)
      logger.info(s"last_written_block=$lastWritten")

      //Fetch blocks until present
      val lastToBeWritten = world.bitcoin.blockCount - world.config.confirmations
      logger.info(s"first_block_to_fetch=${lastWritten+1}, last_block_to_fetch=$lastToBeWritten")

      for (height <- (lastWritten + 1) to lastToBeWritten) {
        logger.debug(s"current_block=$height")
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
