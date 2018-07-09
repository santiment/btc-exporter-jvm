package net.santiment

import java.net.URL
import java.nio.charset.StandardCharsets
import java.util.Base64
import java.util.concurrent.TimeUnit

import com.fasterxml.jackson.databind.ObjectMapper
import com.googlecode.jsonrpc4j.JsonRpcHttpClient
import org.bitcoinj.core._
import org.bitcoinj.params.MainNetParams

import collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global


class BitcoinClient(private val client:JsonRpcHttpClient) {

  val networkParameters: NetworkParameters = MainNetParams.get()

  // The context is a Singleton. Some bitcoinj classes require that it is set up. The next line performs
  // the necessary setup
  val context = new Context(networkParameters)

  val serializer = new BitcoinSerializer(networkParameters,false)

  def getBlockHash(height:Integer):Sha256Hash = {
    client.invoke("getblockhash",Array(height),classOf[Sha256Hash])
  }

  def getBlock(height:Integer):Block = {
    val hash = getBlockHash(height)
    val serializedBlockString = client.invoke("getblock", Array(hash.toString,0), classOf[String])
    val serializedBlock = Utils.HEX.decode(serializedBlockString)
    serializer.makeBlock(serializedBlock)
  }

  def getTx(txHash:Sha256Hash):Transaction = {
    val serialized = Utils.HEX.decode(
      client.invoke("getrawtransaction", Array(txHash.toString,false), classOf[String])
    )

    serializer.makeTransaction(serialized)
  }

  /**
    * Returns the transactions corresponding to a list of hashes. Can be implemented using batching JSON calls in theory.
    * @param hashes - the list of hashes
    * @return - a map of transactions
    */
  def getTxList(hashes:collection.Set[Sha256Hash]):collection.Map[Sha256Hash, Transaction] = {
    val futures = hashes.map {
      hash => Future {
        (hash, getTx(hash))
      }
    }

    Await.result(Future.sequence(futures), Duration.create(30, TimeUnit.SECONDS)).toMap[Sha256Hash, Transaction]
  }

  def blockCount:Int = {
    client.invoke("getblockcount", Array(), classOf[Int])
  }

}
