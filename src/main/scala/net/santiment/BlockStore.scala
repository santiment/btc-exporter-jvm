package net.santiment

import java.{lang, util}

import com.google.common.cache.{CacheBuilder, CacheLoader, CacheStats, LoadingCache}
import com.typesafe.scalalogging.LazyLogging
import org.bitcoinj.core._

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
  * A class for retrieving and caching tx outputs
  *
  * @param client - Bitcoin client used for getting data
  */

class BlockStore(client:BitcoinClient) extends LazyLogging with Periodic[CacheStats] {

  override val period: Int = 60000

  case class OutputKey(hash:Sha256Hash, index:Long)

  object OutputKey {
    def fromOutpoint(o:TransactionOutPoint):OutputKey = OutputKey(o.getHash,o.getIndex)
  }

  // Cache for storing tx outputs
  val outputCache:LoadingCache[OutputKey,TransactionOutput] = CacheBuilder.newBuilder()
    .maximumSize(1000000)
    .initialCapacity(1000000)
    .recordStats()
    .build(new CacheLoader[OutputKey, TransactionOutput] {
      override def load(key: OutputKey): TransactionOutput = {
        val tx = client.getTx(key.hash)
        val output = tx.getOutput(key.index)
        //Remove connection with parent tx to allow for gc
        output.setParent(null)
        output
      }

      override def loadAll(keys: lang.Iterable[_ <: OutputKey]): util.Map[OutputKey, TransactionOutput] = {
        val inputHashes = mutable.HashSet[Sha256Hash]()

        for( key<- keys.asScala ) {
          inputHashes.add(key.hash)
        }

        //Get all parent transactions
        val parents:collection.Map[Sha256Hash,Transaction] = client.getTxList(inputHashes)
        val result = new util.HashMap[OutputKey,TransactionOutput]()
        for (key <- keys.asScala) {
          val output = parents(key.hash).getOutput(key.index)
          output.setParent(null)
          result.put(key,output)
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
        outputCache.put(OutputKey(hash,out.getIndex),out)
        out.setParent(null)
      }
    }

    //Print cache stats
    occasionally( (oldStats:CacheStats)=> {
      val stats = outputCache.stats()
      val diff = if(oldStats != null) {stats.minus(oldStats)} else stats
      logger.info(s"Cache stats: $diff")
      stats
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

  def getOutput(input:TransactionInput):TransactionOutput = {
    val key = OutputKey.fromOutpoint(input.getOutpoint)
    logger.debug(s"get: $key")
    val output = outputCache.get(key)
    outputCache.invalidate(key)
    output
  }

  def blockCount:Int = client.blockCount

}