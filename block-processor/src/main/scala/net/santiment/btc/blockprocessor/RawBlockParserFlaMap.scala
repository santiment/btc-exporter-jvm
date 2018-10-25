package net.santiment.btc.blockprocessor

import net.santiment.btc.BitcoinClient
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.util.Collector
import scala.collection.JavaConverters._

class RawBlockParserFlaMap extends FlatMapFunction[RawBlock, UnmatchedTxEntry] {
  override def flatMap(value: RawBlock, out: Collector[UnmatchedTxEntry]): Unit = {

    //1. Deserialize block
    val block = BitcoinClient.toBlock(value.bytes)
    val txs = block.getTransactions.asScala.zipWithIndex

    val ts = block.getTimeSeconds
    val height = value.height

    //2. Process txs

    txs.foreach{ case (tx,txPos) =>
        //2.1. Emit all inputs
        if (!tx.isCoinBase) {
          tx.getInputs.asScala.foreach { input =>
            out.collect(UnmatchedTxEntry(
              ts = ts,
              height = height,
              txPos = txPos,
              key = OutputKey.fromOutpoint(input.getOutpoint),
              value = null
            ))
          }
        }

        //2.2 Emit all outputs
        tx.getOutputs.asScala.foreach { output =>
          out.collect(UnmatchedTxEntry(
            ts = ts,
            height = height,
            txPos = txPos,
            key = OutputKey.fromOutpoint(output.getOutPointFor),
            value = Output.fromTxOutput(output)
          ))
        }
    }

    //3. Emit final record to indicate that we are done
    out.collect(UnmatchedTxEntry(
      ts = ts,
      height = height,
      txPos = txs.length,
      null,
      null
    ))
  }
}
