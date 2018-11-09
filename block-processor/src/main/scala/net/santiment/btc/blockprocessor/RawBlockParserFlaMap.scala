package net.santiment.btc.blockprocessor

import com.typesafe.scalalogging.LazyLogging
import net.santiment.btc.BitcoinClient
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector
import org.apache.flink.api.scala._

import scala.collection.JavaConverters._

class RawBlockParserFlaMap extends RichFlatMapFunction[RawBlock, UnmatchedTxEntry]
with LazyLogging
{
  @transient private var lastTimestamp: ValueState[java.lang.Long] = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)

    val desc = new ValueStateDescriptor[java.lang.Long](
      "latest-timestamp",
      implicitly[TypeInformation[java.lang.Long]]
    )

    lastTimestamp = getRuntimeContext.getState(desc)
  }

  override def flatMap(value: RawBlock, out: Collector[UnmatchedTxEntry]): Unit = {
    logger.trace(s"Parsing block ${value.height}")
    //1. Deserialize block
    val block = BitcoinClient.toBlock(value.bytes)
    val txs = block.getTransactions.asScala.zipWithIndex

    /**
      * In Bitcoin it can happen that there are consecutive blocks whose timestamps are in reverse order.
      * In order to avoid any problems regarding computation of our metrics down the line, we modify the timestamps of
      * the blocks so that they will be monotonously increasing. The timestamp of the next block will be at least 1
      * second larger than the current block's timestamp.
      */
    val block_ts = block.getTimeSeconds
    if (lastTimestamp.value() == null || lastTimestamp.value().longValue() < block_ts) {
      lastTimestamp.update(Long.box(block_ts))
    } else {
      lastTimestamp.update(lastTimestamp.value()+1L)
    }

    val ts = lastTimestamp.value().longValue()
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
