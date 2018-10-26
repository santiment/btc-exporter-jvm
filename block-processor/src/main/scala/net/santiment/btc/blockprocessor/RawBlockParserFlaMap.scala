package net.santiment.btc.blockprocessor

import com.typesafe.scalalogging.LazyLogging
import net.santiment.btc.BitcoinClient
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.util.Collector

import scala.collection.JavaConverters._

class RawBlockParserFlaMap extends FlatMapFunction[RawBlock, UnmatchedTxEntry]
with LazyLogging
{
  override def flatMap(value: RawBlock, out: Collector[UnmatchedTxEntry]): Unit = {
    logger.trace(s"Parsing block ${value.height}")
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
              key = Option(OutputKey.fromOutpoint(input.getOutpoint)),
              value = None
            ))
          }
        }

        //2.2 Emit all outputs
        tx.getOutputs.asScala.foreach { output =>
          out.collect(UnmatchedTxEntry(
            ts = ts,
            height = height,
            txPos = txPos,
            key = Option(OutputKey.fromOutpoint(output.getOutPointFor)),
            value = Option(Output.fromTxOutput(output))
          ))
        }
    }

    //3. Emit final record to indicate that we are done
    out.collect(UnmatchedTxEntry(
      ts = ts,
      height = height,
      txPos = txs.length,
      None,
      None
    ))
  }
}
