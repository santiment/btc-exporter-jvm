package net.santiment.btc.blockprocessor

import net.santiment.btc.{BitcoinAddress, BitcoinClient}
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector
import org.apache.flink.api.scala._
import org.bitcoinj.core.ScriptException

class TxOutputSpenderFlatmap
extends RichFlatMapFunction[UnmatchedTxEntry, ReducedAccountChange]  {

  @transient private var utxo: ValueState[Output] = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)

    val stateDescriptor = new ValueStateDescriptor[Output](
      "utxo", implicitly[TypeInformation[Output]])

    utxo = getRuntimeContext.getState(stateDescriptor)
  }

  def getAddress(output:Output): String = {

    //The following check is due to tx 59e7532c046ed825683306d6498d886209de02d412dd3f1dc55c55f87ea1c516
    val scriptOpt = try {
      Some(output.parse().script)
    } catch {
      case e: ScriptException => None
    }

    val account = for (script <- scriptOpt) yield BitcoinClient.extractAddress(script)
    account.getOrElse(BitcoinAddress.nullAddress).address
  }

  override def flatMap(value: UnmatchedTxEntry, out: Collector[ReducedAccountChange]): Unit = {

    // If key is null do nothing, this is the dummy entry signifying end of block
    if(value.key == null) {

    }

    // If this is output - store the value in the state and emit the corresponding account change
    else if(value.value != null) {
      utxo.update(value.value)

      out.collect(ReducedAccountChange(
        ts = value.ts,
        height = value.height,
        txPos = value.txPos,
        value = value.value.value,
        address = getAddress(value.value)
      ))
    }

    //If this is an input - spend the corresponding output and emit the corresponding account change
    else {
      val output = utxo.value()

      out.collect(ReducedAccountChange(
        ts = value.ts,
        height = value.height,
        txPos = value.txPos,
        value = -output.value, //convention is to record inputs with negative values
        address = getAddress(output)
      ))
    }
  }
}
