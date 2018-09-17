package net.santiment.btc.blockprocessor

import com.typesafe.scalalogging.LazyLogging
import net.santiment.btc.{BitcoinAddress, BitcoinClient}
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import org.bitcoinj.core._
import org.bitcoinj.script.Script

import collection.JavaConverters._

class BlockProcessorFlatMap
  extends RichFlatMapFunction[RawBlock, AccountChange]()
    with LazyLogging {

  /**
    * Key for the state
    * @param hash
    * @param index
    */
  case class OutputKey(hash: ByteArray, index: Long)

  object OutputKey {
    def fromOutpoint(o:TransactionOutPoint):OutputKey = OutputKey(o.getHash.getBytes,o.getIndex)
  }

  case class ParsedOutput(script:Script, value:Coin)

  case class Output(script:ByteArray, value:Long) {
    def parse() : ParsedOutput  = ParsedOutput(new Script(script), Coin.valueOf(value))
  }

  object Output {
    def fromTxOutput(out:TransactionOutput) : Output = Output(out.getScriptBytes, out.getValue.value)
  }

  val stateDescriptor = new MapStateDescriptor[OutputKey, Output](
    "utxo", implicitly[TypeInformation[OutputKey]], implicitly[TypeInformation[Output]])

  lazy val state: MapState[OutputKey, Output] = {
    val result = getRuntimeContext
      .getMapState(stateDescriptor)
    logger.trace("State connected")
    result
  }

  def spend(o:TransactionOutPoint):ParsedOutput = {
    val key = OutputKey.fromOutpoint(o)
    val result = state.get(key).parse()
    state.remove(key)
    result
  }

  def storeOutput(txOut:TransactionOutput):Unit = {
    val key = OutputKey.fromOutpoint(txOut.getOutPointFor)

    state.put(key, Output.fromTxOutput(txOut))
    logger.trace("stored")
  }


  /**
    * Processes each block, assigning values to each input and emits account changes
    *
    * The method proceeds according to the following algorithm:
    *
    * 1. Deserialize the block
    *    We receive the block in a serialized form from Kafka. The first step is to
    *    deserialize it using bitcoinj
    *
    * 2. Process the coinbase transaction
    *
    *    Store all generated outputs and emit corresponding account changes
    *
    * 3. Process the non-coinbase transactions
    *    For each transaction we need to do the following:
    *
    *    3.1 For each input find and spend the corresponding unspent output in the
    *    state and get its value. Emit the corresponding account change record
    *    3.2 Store each output in the state and emit an account change record
    *    3.3 Calculate the fee that was paid and add it to an accumulator
    *
    * 4. At the end we can calculate how much of the coinbase output value comes from
    *    new coins and how much comes from fees. Accordingly we can add a credit to the
    *    "coinbase" account. The value of the credit will be equal to amount paid to
    *    miner minus collected fees. As a last action emit coinbase account change and
    *    finish
    *
    * @param value
    * @param out
    */
  override def flatMap(value: RawBlock, out: Collector[AccountChange]): Unit = {
    logger.trace(s"Processing block ${value.height}")
    // 1. Deserialize block

    val block = BitcoinClient.toBlock(value.bytes)
    val txs = block.getTransactions.asScala.zipWithIndex
    val ts = block.getTimeSeconds
    val height = value.height


    // 2. Process coinbase tx

    val coinbase = txs.head
    var minerReward = 0L
    var blockFees = 0L

    logger.trace(s"Processing coinbase tx")
    coinbase._1.getOutputs.asScala.zipWithIndex.foreach { case (output, index) =>

      //The following check is due to tx 59e7532c046ed825683306d6498d886209de02d412dd3f1dc55c55f87ea1c516
      val scriptOpt = try {
        Some(output.getScriptPubKey)
      } catch {
        case e: ScriptException => None
      }

      val account = for (script <- scriptOpt) yield BitcoinClient.extractAddress(script)

      val value: Coin = output.getValue

      minerReward += value.getValue

      logger.trace("storing")
      storeOutput(output)

      logger.trace("emitting")
      out.collect(AccountChange(
        in = false, // debit
        ts = block.getTimeSeconds,
        height = height,
        txPos = 0, //coinbase tx is at position 0,
        index = index,
        value = value.toPlainString.toDouble,
        address = account.getOrElse(BitcoinAddress.nullAddress).address
      ))
    }


    //3. Process non-coinbase transactions
    logger.trace(s"Processing non-coinbase txs")
    txs.tail.foreach { case (tx, txPos) =>

      var totalDebit = 0L
      var totalCredit = 0L

      //3.1 Spend and emit all inputs
      tx.getInputs.asScala.zipWithIndex.foreach { case (input, index) =>

        val output = spend(input.getOutpoint)
        val account = BitcoinClient.extractAddress(output.script)
        val value: Coin = output.value

        totalCredit += value.getValue

        out.collect(AccountChange(
          in = true, // credit
          ts = block.getTimeSeconds,
          height = height,
          txPos = txPos,
          index = index,
          //Credits are recoded with negative values
          value = value.negate().toPlainString.toDouble,
          address = account.address
        ))
      }

      //3.2 Store and emit all outputs
      tx.getOutputs.asScala.zipWithIndex.foreach { case (output, index) =>

        //The following check is due to tx ebc9fa1196a59e192352d76c0f6e73167046b9d37b8302b6bb6968dfd279b767
        val scriptOpt = try {
          Some(output.getScriptPubKey)
        } catch {
          case e:ScriptException => None
        }

        val account = for ( script <- scriptOpt ) yield BitcoinClient.extractAddress(script)

        val value:Coin = output.getValue

        totalDebit += value.getValue

        storeOutput(output)

        out.collect(AccountChange(
          in = false, // debit
          ts = block.getTimeSeconds,
          height = height,
          txPos = txPos,
          index = index,
          //Debits are recorded with positive values
          value = value.toPlainString.toDouble,
          address = account.getOrElse(BitcoinAddress.nullAddress).address
        ))
      }

      //3.3 Count fee
      val fee = totalCredit - totalDebit
      blockFees += fee
    }

    //4. Emit coinbase account change - equal to number of minted coins
    logger.trace("Processing fees")
    val coinbaseCredit = Coin.valueOf(minerReward - blockFees)

    out.collect(AccountChange(
      in = true, // credit
      ts = block.getTimeSeconds,
      height = height,
      txPos = txs.size, //virtual tx
      index = 0,
      //Credits are recorded with negative values
      value = coinbaseCredit.negate.toPlainString.toDouble,
      address = "mint"
    ))
  }
}
