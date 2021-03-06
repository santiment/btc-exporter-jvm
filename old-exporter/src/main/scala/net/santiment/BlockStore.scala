package net.santiment

import java.lang
import java.{util => ju}

import com.google.common.cache.{CacheBuilder, CacheLoader, CacheStats, LoadingCache}
import com.typesafe.scalalogging.LazyLogging
import org.bitcoinj.core._
import org.bitcoinj.script.Script

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
  * A class for retrieving and caching tx outputs
  *
  * @param client - Bitcoin client used for getting data
  */

class BlockStore(client:BitcoinClient, cacheSize:Int) extends LazyLogging with Periodic[(CacheStats,BitcoinClientStats)] {

  var invalidated = 0L

  /**
    * Fill the cache with outputs from old blocks
    * @param from
    * @param to
    */
  def precacheBlocks(from: Int, to: Int): Unit = {

    var start = System.currentTimeMillis()
    var total = to - from + 1
    var done = 0

    for (height <- from.to(to) ) {
      //This will retrieve the block and put all of its outputs in the cache
      val block = getBlock(height)

      //We will look if the block's inputs are already in the cache, and if so -- we'll remove them
      for (tx <- block.getTransactions.asScala) {
        for (input <- tx.getInputs.asScala) {
          if (!input.isCoinBase) {
            invalidateIfPresent(input)
          }
        }
      }
      done += 1
      val time = System.currentTimeMillis()
      if (time-start > 60000) {
        logger.info(s"Cached $done out of $total blocks.")
        start = time
      }
    }

  }

  override val period: Int = 60000

  case class OutputKey(hash:Sha256Hash, index:Long)

  object OutputKey {
    def fromOutpoint(o:TransactionOutPoint):OutputKey = OutputKey(o.getHash,o.getIndex)
  }

  case class ParsedOutput(script:Script, value:Coin)

  case class Output(script:Array[Byte], value:Long) {
    def parse() : ParsedOutput  = ParsedOutput(new Script(script), Coin.valueOf(value))
  }

  object Output {
    def fromTxOutput(out:TransactionOutput) : Output = Output(out.getScriptBytes, out.getValue.value)
  }

  // Cache for storing tx outputs
  val outputCache:LoadingCache[OutputKey,Output] = CacheBuilder.newBuilder()
    .maximumSize(cacheSize)
    .initialCapacity(cacheSize)
    .recordStats()
    .build(new CacheLoader[OutputKey, Output] {
      override def load(key: OutputKey): Output = {
        val tx = client.getTx(key.hash)
        val output = tx.getOutput(key.index)
        //Remove connection with parent tx to allow for gc
        output.setParent(null)
        Output.fromTxOutput(output)
      }

      override def loadAll(keys: lang.Iterable[_ <: OutputKey]): ju.Map[OutputKey, Output] = {
        val inputHashes = mutable.HashSet[Sha256Hash]()

        for( key<- keys.asScala ) {
          inputHashes.add(key.hash)
        }

        //Get all parent transactions
        val parents:collection.Map[Sha256Hash,Transaction] = client.getTxList(inputHashes)
        val result = new ju.HashMap[OutputKey,Output]()
        for (key <- keys.asScala) {
          val output = parents(key.hash).getOutput(key.index)
          output.setParent(null)
          result.put(key,Output.fromTxOutput(output))
        }

        result
      }
    })


  def getBlock(height:Integer): Block = {
    val block = client.getBlock(height)

    // We will disconnect here all inputs and outputs from their parent transactions.
    // This will allow the garbage collector to clean the tx objects even if we are
    // storing the inputs or outputs

    for (tx<- block.getTransactions.asScala) {
      val hash = tx.getHash

      for (in <- tx.getInputs.asScala) {
        in.setParent(null)
      }

      for(out <- tx.getOutputs.asScala) {
        //Cache the output then remove connection with parent
        outputCache.put(OutputKey(hash,out.getIndex),Output.fromTxOutput(out))
        out.setParent(null)
      }
    }

    //Print cache stats
    occasionally( oldStats => {
      val cacheStats = outputCache.stats()
      val btcStats = client.stats.copy()
      val cacheDiff = if (oldStats != null) {
        cacheStats.minus(oldStats._1)
      } else cacheStats
      val btcDiff = if (oldStats != null) {
        btcStats.minus(oldStats._2)
      } else btcStats
      logger.info(s"Cache stats: $cacheDiff")
      logger.info(s"Cache size: ${outputCache.size}, invalidated: ${invalidated}")
      logger.info(s"Bitcoin client stats: $btcDiff")

      invalidated = 0
      (cacheStats, btcStats)
    })

    block
  }

  /**
    * Get and cache all outputs connected to the inputs in the given transactions
    * @param txs
    */
  def cacheOutputs(txs: lang.Iterable[_ <: Transaction]): Unit = {
    val keys = for (
      tx <- txs.asScala;
      input <- tx.getInputs.asScala
    ) yield {
      OutputKey.fromOutpoint(input.getOutpoint)
    }

    if (keys.nonEmpty) {
      logger.debug(s"getAll: $keys")
      outputCache.getAll(keys.asJava)
    }
  }

  def getOutput(input:TransactionInput):ParsedOutput = {
    val key = OutputKey.fromOutpoint(input.getOutpoint)
    logger.debug(s"get: $key")

    val output = outputCache.get(key)
    outputCache.invalidate(key)
    invalidated += 1
    output.parse()
  }

  def invalidateIfPresent(input:TransactionInput):Unit = {
    val key = OutputKey.fromOutpoint(input.getOutpoint)

    val output = outputCache.getIfPresent(key)
    if (output != null) {
      outputCache.invalidate(key)
      invalidated += 1
    }
  }

  def blockCount:Int = client.blockCount

}