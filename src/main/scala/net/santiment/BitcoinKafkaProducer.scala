package net.santiment

import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.common.KafkaException
import org.bitcoinj.core._
import org.bitcoinj.core.TransactionInput.{ConnectMode, ConnectionResult}

import collection.JavaConverters._
import scala.collection.mutable
import java.{util => j}

import scala.concurrent.Future

class BitcoinKafkaProducer
(
  world : {
    val config: Config
    val bitcoin: BlockStore
    val lastBlockStore: TransactionalStore[Int]
    val sink: TransactionalSink[ResultTx]
    def closeEverythingQuietly (): Unit
    val migrator: Migrator
  }
)
  extends LazyLogging
{

  var sinkCommitted = false

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

    logger.trace(s"amounts=$amounts")

    //2.Order debits and credits by amount and match them greedily
    case class Entry(account:BitcoinAddress, var value:Long)

    val debits2 = mutable.PriorityQueue[Entry]()(Ordering.by(_.value))
    val credits2 = mutable.PriorityQueue[Entry]()(Ordering.by(0L-_.value))
    var total = 0L

    for ( (address,value) <- amounts ) {
      if(value>0L) {
        debits2.enqueue(Entry(address,value))
        total += value
      } else if (value<0L) {
        credits2.enqueue(Entry(address,value))
      }
    }

    var curDebit:Entry = Entry(null,0L)
    var curCredit:Entry = Entry(null,0L)
    var result = new j.LinkedList[(BitcoinAddress,BitcoinAddress,Coin)]()

    while (total > 0L) {
      logger.trace(s"loop $curDebit, $curCredit, $total")
      if(curDebit.value == 0L) {
        curDebit = debits2.dequeue()
      }
      if(curCredit.value == 0L) {
        curCredit = credits2.dequeue()
      }

      val transferValue = Math.min(curDebit.value,-curCredit.value)
      curDebit.value -= transferValue
      curCredit.value += transferValue
      result.add( (curCredit.account,
        curDebit.account,
        Coin.valueOf(transferValue)))

      total -= transferValue

    }

    if (debits2.nonEmpty || credits2.nonEmpty) {
      throw new IllegalStateException("Stacks don't match")
    }

    result
  }

  def processBlock(height: Int): Int = {
    logger.debug(s"current_block=$height, begin_process")
    val block = world.bitcoin.getBlock(height)

    val txs: j.List[Transaction] = block.getTransactions
    val nonCoinbaseTxs = txs.subList(1,txs.size())
    val coinbase = txs.get(0)

    var pushedEvents = 0L
    var blockFees = 0L

    logger.trace(s"current_block=$height, fetched_txs=${txs.size()}")
    world.bitcoin.cacheOutputs(nonCoinbaseTxs)

    //Process all non-coinbase txs and calculate the fees
    for(tx:Transaction <- nonCoinbaseTxs.asScala) {

      var totalDebit = 0L
      var totalCredit = 0L

      //Get list of debits and credits
      var debits = for(output:TransactionOutput <- tx.getOutputs.asScala) yield {

        //The following check is due to tx ebc9fa1196a59e192352d76c0f6e73167046b9d37b8302b6bb6968dfd279b767
        val scriptOpt = try {
          Some(output.getScriptPubKey)
        } catch {
          case e:ScriptException => None
        }

        val account = for ( script <- scriptOpt ) yield BitcoinClient.extractAddress(script)

        val value:Coin = output.getValue

        totalDebit += value.getValue
        TransactionEntry(account.getOrElse(BitcoinAddress.nullAddress),value)
      }

      //Connect input to parents and return input list
      val credits = for(input:TransactionInput <- tx.getInputs.asScala) yield {
        val output = world.bitcoin.getOutput(input)

        val account = BitcoinClient.extractAddress(output.script)
        val value: Coin = output.value


        //We store credits as negative values
        totalCredit -= value.getValue
        TransactionEntry(account, value.negate())
      }

      //Compute fees. The fee for each non-coinbase tx is equal to the difference between the debits and the credits
      val fee = 0-(totalDebit + totalCredit)
      if(fee < 0) {
        throw new IllegalStateException("credit < debit")
      }

      val feeDebit = TransactionEntry(BitcoinAddress(s"fee","virtual"),Coin.valueOf(fee))
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
    var minerReward = 0L

    val cbDebits = for(output:TransactionOutput <- coinbase.getOutputs.asScala) yield {
      val account = BitcoinClient.extractAddress(output.getScriptPubKey)
      val value:Coin = output.getValue

      minerReward += value.getValue
      TransactionEntry(account,value)
    }

    val minted = - (blockFees + minerReward) //credit, i.e. negative

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

    txs.size()
  }

  def makeTxProcessor(lastBlock:TransactionalStore[Int], sink:TransactionalSink[ResultTx], processor:Int=>Int)(height:Int): Int = {
    //Start a new transaction
    sink.begin()
    logger.trace(s"current_block=$height, begin_transaction")
    lastBlock.begin()

    //Process the current block and send the resulting records to Kafka
    val result = processor(height)
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

    result
  }

  lazy val txProcess: Int => Int = makeTxProcessor(world.lastBlockStore, world.sink, processBlock)

  /**
   * Main function. Should be called from a cron job which runs once every 10 minutes
   */

  def main(args: Array[String]): Unit = {
    logger.info(s"Starting BTC exporter ${BuildInfo.version}")

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
      val lastToBeWritten = world.bitcoin.blockCount - world.config.confirmations
      logger.info(s"first_block_to_fetch=${lastWritten+1}, last_block_to_fetch=$lastToBeWritten")

      var startTs = System.currentTimeMillis()
      var blocks = 0
      var txs = 0
      for (height <- (lastWritten + 1) to lastToBeWritten) {
        logger.debug(s"current_block=$height")
        txs += txProcess(height)
        blocks += 1
        var test = System.currentTimeMillis()
        if (test - startTs > 60000) {
          logger.info(s"blocks=$blocks, txs=$txs, interval=${(test-startTs)/1000}s, last_block=$height")
          startTs = test
          blocks = 0
          txs = 0
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
