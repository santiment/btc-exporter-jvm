package net.santiment

import java.net.URL
import java.nio.charset.StandardCharsets
import java.util.Base64
import java.util.concurrent.TimeUnit

import com.fasterxml.jackson.databind.ObjectMapper
import com.googlecode.jsonrpc4j.JsonRpcHttpClient
import com.typesafe.scalalogging.LazyLogging
import org.bitcoinj.core._
import org.bitcoinj.params.MainNetParams
import org.bitcoinj.script.Script

import collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global


class BitcoinClient(private val client:JsonRpcHttpClient) {

  def getBlockHash(height:Integer):Sha256Hash = {
    client.invoke("getblockhash",Array(height),classOf[Sha256Hash])
  }

  def getBlock(height:Integer):Block = {
    val hash = getBlockHash(height)
    val serializedBlockString = client.invoke("getblock", Array(hash.toString,0), classOf[String])
    val serializedBlock = Utils.HEX.decode(serializedBlockString)
    BitcoinClient.serializer.makeBlock(serializedBlock)
  }

  def getTx(txHash:Sha256Hash):Transaction = {
    val serialized = Utils.HEX.decode(
      client.invoke("getrawtransaction", Array(txHash.toString,false), classOf[String])
    )

    BitcoinClient.serializer.makeTransaction(serialized)
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

case class BitcoinAddress(address:String, kind:String)

object BitcoinClient extends LazyLogging {

  val mainNetParams: NetworkParameters = MainNetParams.get()

  // The context is a Singleton. Some bitcoinj classes require that it is set up. The next line performs
  // the necessary setup
  val context = new Context(mainNetParams)

  val serializer = new BitcoinSerializer(mainNetParams,false)

  def extractAddress(scriptPubKey:Script):BitcoinAddress = {

    //1. P2PKH
    if( scriptPubKey.isSentToAddress) {
      val address = scriptPubKey.getToAddress(mainNetParams)
      return BitcoinAddress(address.toBase58,"P2PKH")
    }

    //2. P2PK (We still convert to newform addresses
    if(scriptPubKey.isSentToRawPubKey) {
      val address = scriptPubKey.getToAddress(mainNetParams, true)
      return BitcoinAddress(address.toBase58, "P2PK")
    }

    //3. P2SH
    if(scriptPubKey.isPayToScriptHash) {
      val address = scriptPubKey.getToAddress(mainNetParams)
      return BitcoinAddress(address.toBase58, "P2SH")
    }

    /** 4. Witness address. The witness scriptPubKey has the following format:
      * `witness_version payload`
      *
      * where:
      * - `witness_version` is 1 byte between 0 and 16. Only existing current version is 1
      * - `payload` is between 2 and 40 bytes. Right now only lengths 20 and 32 are supported
      *
      * For more info see BIP141
      */

    val program = scriptPubKey.getProgram
    def isSegWit(program:Array[Byte]):Boolean = {
        program(0) <= 0x10 &&
          program(1) >= 0x02 &&
          program(1) <= 0x28 &&
          program.length == 2+program(1)
    }

    def isP2WKH(program:Array[Byte]): Boolean =
      program(0) == 0 &&
        program(1) == 0x14

    def isP2WSH(program:Array[Byte]): Boolean =
      program(0) == 0 &&
        program(1) == 0x20

    /**
      * The segwit output scripts can be represented as *Bech32 addresses*
      *
      * Their format is described in BIP173
      */


    if(isSegWit(program)) {

      // The bech32 encoding is copied from the master branch of bitcoinj
      // Unfortunately it's not released yet
      val data = Bech32.convertBits(program,2,program.length-2,8,5,true)
      //Add the witness version (0) to the head of the array
      val fullData = new Array[Byte](1+data.length)
      System.arraycopy(data,0,fullData,1,data.length)
      fullData(0)=0 //The current witness version
      val addr = Bech32.encode("bc",fullData)
            //4.1 P2WKH
      if(isP2WKH(program)) {
        return BitcoinAddress(addr, "P2WKH")
      }

      //4.2 P2SKH
      if(isP2WSH(program)) {
        return BitcoinAddress(addr, "P2WSH")
      }

      throw new ScriptException(s"Unknown witness script ${scriptPubKey.toString}")
    }

    //Unsupported address types
    throw new ScriptException(s"Unknown script ${scriptPubKey.toString}")

  }


}
