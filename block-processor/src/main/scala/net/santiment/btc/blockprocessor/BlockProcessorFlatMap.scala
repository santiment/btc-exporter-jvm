package net.santiment.btc.blockprocessor

import com.typesafe.scalalogging.LazyLogging
import net.santiment.btc.{BitcoinAddress, BitcoinClient}
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import org.bitcoinj.core._
import org.bitcoinj.script.Script

import collection.JavaConverters._

class BlockProcessorFlatMap
  extends RichFlatMapFunction[RawBlock, ProcessedTx]()
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

  @transient private var state:MapState[OutputKey, Output] = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)

    val stateDescriptor = new MapStateDescriptor[OutputKey, Output](
    "utxo", implicitly[TypeInformation[OutputKey]], implicitly[TypeInformation[Output]])

    state = getRuntimeContext.getMapState(stateDescriptor)
    logger.trace("Connected")
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
  override def flatMap(value: RawBlock, out: Collector[ProcessedTx]): Unit = {
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
    val coinbaseOutputs = new Array[TxEntry](coinbase._1.getOutputs.size())
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

      coinbaseOutputs(index) = TxEntry(
        address = account.getOrElse(BitcoinAddress.nullAddress).address,
        value = value.value
      )
    }

    out.collect(ProcessedTx(
      ts = ts,
      height = height,
      txPos = 0,
      inputs = new Array(0),
      outputs = coinbaseOutputs
    ))

    //3. Process non-coinbase transactions
    logger.trace(s"Processing non-coinbase txs")
    txs.tail.foreach { case (tx, txPos) =>

      var totalDebit = 0L
      var totalCredit = 0L

      val inputs = new Array[TxEntry](tx.getInputs.size())
      val outputs = new Array[TxEntry](tx.getOutputs.size())

      //3.1 Spend and all inputs
      tx.getInputs.asScala.zipWithIndex.foreach { case (input, index) =>

        val output = spend(input.getOutpoint)
        val account = BitcoinClient.extractAddress(output.script)
        val value: Coin = output.value

        totalCredit += value.getValue

        inputs(index) = TxEntry(
          value = value.value,
          address = account.address

        )
      }

      //3.2 Store and all outputs
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

        outputs(index) = TxEntry(
          address = account.getOrElse(BitcoinAddress.nullAddress).address,
          value = value.value
        )
      }

      //3.3 Count fee
      val fee = totalCredit - totalDebit
      blockFees += fee

      //3.4. Emit the resulting processed transaction
      out.collect(ProcessedTx(
        ts = ts,
        height = height,
        txPos = txPos,
        inputs = inputs,
        outputs = outputs
      ))
    }

    //4. Emit coinbase account change - equal to number of minted coins
    logger.trace("Processing fees")
    val coinbaseCredit = Coin.valueOf(minerReward - blockFees)

    out.collect(ProcessedTx(
      ts = ts,
      height = height,
      txPos = txs.size,
      inputs = Array(TxEntry(
        address = "mint",
        value = coinbaseCredit.value
      )),
      outputs = new Array(0)
    ))
  }
}
