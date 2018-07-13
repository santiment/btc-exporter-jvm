package net.santiment

import java.util.concurrent.TimeUnit

import com.googlecode.jsonrpc4j.JsonRpcHttpClient
import com.typesafe.scalalogging.LazyLogging
import org.bitcoinj.core._
import org.bitcoinj.params.MainNetParams
import org.bitcoinj.script.Script

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}


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

  /**
    * Copied from the master branch of bitcoinj. Unfortunately it is not released yet
    */

  def extractAddress(scriptPubKey:Script):BitcoinAddress = scriptPubKey match {

    //1. P2PKH
    case script if ScriptPattern.isPayToPubKeyHash(script) =>
      val address = script.getToAddress(mainNetParams)
      BitcoinAddress(address.toBase58,"P2PKH")

    //2. P2PK (We still convert to newform addresses
    case script if ScriptPattern.isPayToPubKey(script) =>
      val address = script.getToAddress(mainNetParams, true)
      BitcoinAddress(address.toBase58, "P2PK")


    //3. P2SH
    case script if ScriptPattern.isPayToScriptHash(script) =>
      val address = script.getToAddress(mainNetParams)
      BitcoinAddress(address.toBase58, "P2SH")


    //4. P2W
    case script if ScriptPattern.isPayToWitnessHash(script) =>
      // The bech32 encoding is copied from the master branch of bitcoinj
      // Unfortunately it's not released yet
      val program = script.getProgram
      val data = Bech32.convertBits(program,2,program.length-2,8,5,true)
      //Add the witness version (0) to the head of the array
      val fullData = new Array[Byte](1+data.length)
      System.arraycopy(data,0,fullData,1,data.length)
      fullData(0)=0 //The current witness version
      val addr = Bech32.encode("bc",fullData)

      //4.1 P2WPKH
      if(ScriptPattern.isPayToWitnessPubKeyHash(script)) BitcoinAddress(addr, "P2WPKH")

      //4.2 P2WSH
      else if(ScriptPattern.isPayToWitnessScriptHash(script)) BitcoinAddress(addr, "P2WSH")

      else throw new ScriptException(s"Unknown witness script $script")

    //5. Null script
    case script if ScriptPattern.isOpReturn(script) =>
      BitcoinAddress("","NULL")

    //Unsupported address types
    case script =>
      throw new ScriptException(s"Unknown script ${script.toString}")
  }


}
