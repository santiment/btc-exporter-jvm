package net.santiment

import java.util.Base64

import com.googlecode.jsonrpc4j.JsonRpcHttpClient
import com.typesafe.scalalogging.LazyLogging
import org.bitcoinj.core._
import org.bitcoinj.params.MainNetParams
import org.bitcoinj.script.Script

case class BitcoinClientStats
(
  var getBlock:Int = 0,
  var getBlockTime:Long = 0L,
  var getTx:Int = 0,
  var getTxTime: Long = 0L,
  var getTxList: Int = 0,
  var getTxListTime: Long = 0L,
  var getTxListSize: Long = 0L
) {

  def minus(other:BitcoinClientStats):BitcoinClientStats = BitcoinClientStats(
    getBlock - other.getBlock,
    getBlockTime - other.getBlockTime,
    getTx - other.getTx,
    getTxTime - other.getTxTime,
    getTxList - other.getTxList,
    getTxListTime - other.getTxListTime,
    getTxListSize - other.getTxListSize
  )

  override def toString: String = {
    s"BitcoinClientStats(getBlock=$getBlock, getBlockTime=$getBlockTime, getTx=$getTx, getTxTime=$getTxTime, getTxList=$getTxList, getTxListTime=$getTxListTime, getTxListSize=$getTxListSize)"
  }

}

class BitcoinClient(private val client:JsonRpcHttpClient,
                    private val batchClient: BatchJsonRPCClient)
extends LazyLogging {

  val stats: BitcoinClientStats = BitcoinClientStats()

  def getBlockHash(height:Integer):Sha256Hash = {
    client.invoke("getblockhash",Array(height),classOf[Sha256Hash])
  }

  def getBlock(height:Integer):Block = {
    val start = System.nanoTime()
    val hash = getBlockHash(height)
    val serializedBlockString = client.invoke("getblock", Array(hash.toString,0), classOf[String])
    val end = System.nanoTime()
    val serializedBlock = Utils.HEX.decode(serializedBlockString)
    val block = BitcoinClient.serializer.makeBlock(serializedBlock)

    stats.getBlock += 1
    stats.getBlockTime += (end-start)

    block
  }

  def _getTx(txHash:Sha256Hash):Transaction = {

    val response = client.invoke("getrawtransaction", Array(txHash.toString,false), classOf[String])
    val serialized = Utils.HEX.decode(response)

    val tx = BitcoinClient.serializer.makeTransaction(serialized)

    tx
  }

  def getTx(txHash:Sha256Hash):Transaction = {

    val start = System.nanoTime()
    val tx = _getTx(txHash)
    val end = System.nanoTime()
    stats.getTx +=1
    stats.getTxTime += (end-start)

    tx
  }

  /**
    * Returns the transactions corresponding to a list of hashes. Can be implemented using batching JSON calls in theory.
    * @param hashes - the list of hashes
    * @return - a map of transactions
    */
  def getTxList(hashes:collection.Set[Sha256Hash]):collection.Map[Sha256Hash, Transaction] = {

    val hashArray = hashes.toArray
    val size = hashArray.length
    val requests = for (hash<- hashArray) yield {
      Array[Object](hash.toString, Predef.boolean2Boolean(false))
    }

    val start = System.nanoTime()
    logger.debug("Invoking BatchRPC call")
    val responseArray = batchClient.invoke[String]("getrawtransaction", requests, Map())
    logger.debug("Result received")
    val end = System.nanoTime()

    val resultArr = for (i <- 0 until size ) yield {
      val serialized = Utils.HEX.decode(responseArray(i))
      val tx = BitcoinClient.serializer.makeTransaction(serialized)
      (hashArray(i), tx)
    }

    // `:_*` allows us to pass an array to a vararg function
    val result = Map[Sha256Hash,Transaction](resultArr:_*)

    stats.getTxList += 1
    stats.getTxListTime += (end-start)
    stats.getTxListSize += size

    result
  }

  def blockCount:Int = {
    client.invoke("getblockcount", Array(), classOf[Int])
  }
}

case class BitcoinAddress(address:String, kind:String)

object BitcoinAddress {
  val nullAddress: BitcoinAddress = BitcoinAddress("","NULL")
}


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
      try {
        val address = script.getToAddress(mainNetParams, true)
        BitcoinAddress(address.toBase58, "P2PK")
      } catch {
        case _:IllegalArgumentException =>
          //The cause for this case is tx b728387a3cf1dfcff1eef13706816327907f79f9366a7098ee48fc0c00ad2726
          val address = new String(Base64.getEncoder.encode(script.getProgram))
          BitcoinAddress(s"unknown:invalidpubkey:$address","UNKNOWN")
      }


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
      BitcoinAddress.nullAddress

    //6. Old-style Multisig script
    case script if ScriptPattern.isSentToMultisig(script) =>
      val chunks = script.getChunks

      val numKeys = ScriptPattern.decodeFromOpN(chunks.get(chunks.size-2).opcode)
      val numSigs = ScriptPattern.decodeFromOpN(chunks.get(0).opcode)

      val addresses: Seq[String] = (for(i<- 1 to numKeys) yield {
        val chunk = chunks.get(i)
        try {
          val addr = ECKey.fromPublicOnly(chunk.data).toAddress(mainNetParams)
          Seq[String](addr.toBase58)
        } catch {
          case e:IllegalArgumentException =>
            //added to deal with tx 274f8be3b7b9b1a220285f5f71f61e2691dd04df9d69bb02a8b3b85f91fb1857
            //Any invalid address cannot be used to spend a multisig tx. But the valid addresses still can be used
            Seq[String]()
        }
      }).flatten

      // If after the above procedure we get only one valid address and numSigs is 1, then
      // we will treat this as a normal address
      if(numSigs == 1 && addresses.size == 1) {
        BitcoinAddress(addresses.head,"MULTISIG")
      } else {
        val addrstr = addresses.mkString(":")
        BitcoinAddress(s"multisig:$numSigs:$numKeys:$addrstr", "MULTISIG")
      }


    case script =>
      val address = new String(Base64.getEncoder.encode(script.getProgram))
      BitcoinAddress(s"unknown:$address", "UNKNOWN")

  }

}
