package net.santiment.btc.blockprocessor

import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.scala.{DataStream, _}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.Window
import org.apache.flink.util.Collector
import org.bitcoinj.core.Coin

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class BlockProcessor
(
  var context:Option[Context] = None
)
  extends LazyLogging
{
  def main(args: Array[String]): Unit = {

    val ctx = context.getOrElse(new Context(args))

    //Create kafka topic for storing btc transfers if not already created
    ctx.migrator.up()

    // Parse and extract the inputs and outputs from each block. This is the only operation which cannot be parallelised
    // with the current algorithm. However it requires no state, so it should go pretty fast.
    val unprocessedEntries = ctx.rawBlockSource
      .flatMap(new RawBlockParserFlaMap())
      .uid("block-parser-flatmap")
      .name("block-parser")
      .setParallelism(1)
      .assignTimestampsAndWatermarks( new AssignerWithPunctuatedWatermarks[UnmatchedTxEntry] with LazyLogging {
        override def checkAndGetNextWatermark(lastElement: UnmatchedTxEntry, extractedTimestamp: Long): Watermark = {
          if(lastElement.key.isEmpty)
            // This is the last tx for this block. We emit a watermark here. We assume that the timestamp of the next
            // block will be at least 1 second bigger. Otherwise there could be problems
            new Watermark(extractedTimestamp+999)
          else
            null
        }

        override def extractTimestamp(element: UnmatchedTxEntry, previousElementTimestamp: Long): Long = {
          element.ts * 1000 //Timestamps are given in seconds so we need to convert to ms
        }
      })
      .uid("assign-timestamps/watermarks")
      .name("timestamps/watermarks")
      .setParallelism(1)

    /**
      * Match tx inputs and outputs to extract the input's value and bitcoin address. This requires state.
      */
    val processedEntries = unprocessedEntries
      .keyBy(_.key)
      .flatMap( new TxOutputSpenderFlatmap())
      .uid("tx-output-spender-flatmap")
      .name("tx-output-spender")

    //processedEntries.print()

    /**
      * Sort account changes for each account within each block. Also reduce entries which have both input
      * and output in the same transaction. This operation in network IO intensive since it requires the inputs
      * and outputs to be reshuffled and keyed by address.
      */
    val sortedAndReducedChanges:DataStream[ReducedAccountChange] = processedEntries
      .keyBy(_.address)
      .timeWindow(Time.milliseconds(1))
      .apply[ReducedAccountChange] {
      (
        address:String,
        window:Window,
        entries:Iterable[ReducedAccountChange],
        out:Collector[ReducedAccountChange]
      ) => {
        val result = entries
          .groupBy(_.txPos)
          .map { case (txPos, xs) =>
            val value = xs.view.map(_.value).sum
            ReducedAccountChange(
              ts = xs.head.ts,
              height = xs.head.height,
              txPos = txPos,
              value = value,
              address = address
            )
          }
          .toArray
          .sortBy(_.txPos)

        for (entry <- result) {
          if (entry.value != 0L) {
            out.collect(entry)
          }
        }
      }
    }

      .uid("sort-and-reduce-obvious-change")
      .name("sort-and-reduce-obvious-change")




/*
    // Convert processed transactions to list of account changes. Output first the inputs, then the outputs.

    def extractAccountChanges(txs: DataStream[ProcessedTx]):DataStream[AccountChange] = {
      txs.flatMap { tx =>
        val inputs = tx.inputs.zipWithIndex.map { case (entry, index) =>
          AccountChange(
            in = true,
            ts = tx.ts,
            height = tx.height,
            txPos = tx.txPos,
            index = index,
            value = Coin.valueOf(entry.value).negate().toPlainString.toDouble,
            address = entry.address
          )
        }

        val outputs = tx.outputs.zipWithIndex.map { case (entry, index) =>
          AccountChange(
            in = false,
            ts = tx.ts,
            height = tx.height,
            txPos = tx.txPos,
            index = index,
            value = Coin.valueOf(entry.value).toPlainString.toDouble,
            address = entry.address
          )
        }
        inputs ++ outputs
      }
    }
*/


    val accountChanges:DataStream[AccountChange] = sortedAndReducedChanges.map { in =>
      AccountChange(
        in = in.value < 0,
        ts = in.ts,
        height = in.height,
        txPos = in.txPos,
        value = Coin.valueOf(in.value).toPlainString.toDouble,
        address = in.address
      )
    }
      .name("account-chaanges")
      .uid("account-changes")


    /**
      * Compute transaction stack changes
      */
    val stackChanges = sortedAndReducedChanges
      .keyBy(_.address)
      .flatMap(new TransactionStackFlatMap())
      .uid("transaction-stack-flatmap")
      .name("transaction-stack-processor")



    //stackChanges.print()
    //Compute token circulation
    //TODO


    //Send to the kafka sink
    //    ctx.consumeTransfers(accountChanges)

    //sortedAndReducedChanges.print()
    val txVolume = sortedAndReducedChanges
      .keyBy(_.height)
      .timeWindow(Time.milliseconds(2))
      .reduce { (left, right)=>
        //System.out.println(s"s left: ${left}, right: ${right}")
        var total = 0L
        if(left.value < 0)
          total += left.value
        if(right.value < 0)
          total += right.value
        ReducedAccountChange(
          ts = left.ts,
          height = left.height,
          txPos = -1,
          value = total,
          address = "trivial-transaction-volume"
        )
      }

    ctx.consumeTransfers(accountChanges)
    ctx.consumeStackChanges(stackChanges)

    //processedTxs.print()


    ctx.execute("btc-block-processor")
  }
}

object Main extends BlockProcessor() {}

