package net.santiment

import java.net.HttpURLConnection
import java.util.Base64
import java.util.concurrent.{ExecutionException, TimeUnit}

import com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache}
import com.googlecode.jsonrpc4j.JsonRpcHttpClient
import com.typesafe.scalalogging.LazyLogging
import org.bitcoinj.core._
import org.bitcoinj.params.MainNetParams
import org.bitcoinj.script.Script

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.collection.JavaConverters._

case class BitcoinClientStats
(
  var getRawBlock:Int = 0,
  var getRawBlockTime:Long = 0L
) {

  def minus(other:BitcoinClientStats):BitcoinClientStats = BitcoinClientStats(
    getRawBlock - other.getRawBlock,
    getRawBlockTime - other.getRawBlockTime
  )

  override def toString: String = {
    s"BitcoinClientStats(getRawBlock=$getRawBlock, getRawBlockTime=$getRawBlockTime)"
  }

}

class BitcoinClient(private val client:JsonRpcHttpClient,
                    private val batchClient: BatchJsonRPCClient)

extends LazyLogging
with Periodic[BitcoinClientStats] {

  val stats: BitcoinClientStats = BitcoinClientStats()
  override val period: Int = 60000

  def getBlockHash(height:Integer):Sha256Hash = {
    client.invoke("getblockhash",Array(height),classOf[Sha256Hash])
  }

  def getBlock(height:Integer):Block = {
    BitcoinClient.serializer.makeBlock(getRawBlock(height))
  }

  def getRawBlock(height: Integer): Array[Byte] = {
    val start = System.nanoTime()
    val hash = getBlockHash(height)
    val serializedBlockString = client.invoke("getblock", Array(hash.toString,0), classOf[String])
    val end = System.nanoTime()
    val serializedBlock = Utils.HEX.decode(serializedBlockString)

    stats.getRawBlock += 1
    stats.getRawBlockTime += (end-start)


    //Print cache stats
    occasionally( oldStats => {
      val btcStats = stats.copy()
      val btcDiff = if (oldStats != null) {
        btcStats.minus(oldStats)
      } else btcStats
      logger.info(s"Bitcoin client stats: $btcDiff")
      btcStats
    })

    serializedBlock
  }

  def blockCount:Int = {
    client.invoke("getblockcount", Array(), classOf[Int])
  }
}



object BitcoinClient extends LazyLogging {

  val mainNetParams: NetworkParameters = MainNetParams.get()

  // The context is a Singleton. Some bitcoinj classes require that it is set up. The next line performs
  // the necessary setup
  val context = new Context(mainNetParams)

  val serializer = new BitcoinSerializer(mainNetParams,false)

}
