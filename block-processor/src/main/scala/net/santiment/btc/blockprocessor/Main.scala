package net.santiment.btc.blockprocessor

import com.typesafe.scalalogging.LazyLogging
import net.santiment.util.Migrator
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, GlobalWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger
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

    //This job is not parallel
    ctx.env.setParallelism(1)


    val processedTxs = ctx.rawBlockSource
      // Dummy key which allows us to use keyed map state
      .keyBy(_ =>())

      // Extract account changes
      .flatMap(new BlockProcessorFlatMap())(implicitly[TypeInformation[ProcessedTx]])
      .uid("block-processor-flatmap")
      .name("block-processor")

      // Assign timestamps and watermarks. The timestamps will be recorded in the kafka topic
      .assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks[ProcessedTx] {
          override def checkAndGetNextWatermark(lastElement: ProcessedTx, extractedTimestamp: Long): Watermark = {
            // Generate watermark at the last entry from each block
            if(lastElement.inputs.length == 1 && lastElement.inputs(0).address == "mint")
              // This is the last tx for this block (the virtual mint transaction). We emit a watermark here
              // We assume that the timestamp of the next block will be at least 1 second bigger. Otherwise there could
              // be problems
              new Watermark(extractedTimestamp+999)
            else
              null


          }
          override def extractTimestamp(element: ProcessedTx, previousElementTimestamp: Long): Long = {
            //timestamps are given in seconds so we need to convert to ms
            element.ts * 1000
          }
        })

    //Compute reduced transactions - that is we take into account the obvious change.
    val reducedTxs = processedTxs.map {tx =>
      /*
        There is a way to do the reduction inplace and to use the input and output arrays for it. We
        would first need to sort the input and output arrays by address and then do the reduction in some smart
        way. After the sorting, the reduction operation can be done with linear speed. The benefit of such a method
        is that it would some GC usage. We might consider doing that later, if GC becomes a bottleneck.
       */

      val amounts:mutable.HashMap[String, Long] = mutable.HashMap()

      for (te <- tx.inputs) {
        val old = amounts.getOrElse(te.address, 0L)
        amounts.put(te.address, old-te.value)
      }

      for (te <- tx.outputs) {
        val old = amounts.getOrElse(te.address, 0L)
        amounts.put(te.address, old+te.value)
      }

      val inputs = ArrayBuffer[TxEntry]()
      val outputs = ArrayBuffer[TxEntry]()

      for ( (address, value) <- amounts ) {
        if(value < 0L) {
          inputs.append(TxEntry(
            address = address,
            value = 0L-value
          ))
        }

        if(value > 0L) {
          outputs.append(TxEntry(
            address = address,
            value = value
          ))
        }
      }

      ProcessedTx(
        ts = tx.ts,
        height = tx.height,
        txPos = tx.txPos,
        inputs = inputs.toArray,
        outputs = outputs.toArray
      )
    }
      .name("reduce-obvious-change")
      .uid("reduce-obvious-change")


    // Convert processed transactoins to list of account changes. Output first the inputs, then the outputs
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

    val accountChanges:DataStream[AccountChange] = extractAccountChanges(processedTxs)
      .name("account-chaanges")
      .uid("account-changes")


    // Convert processed transactoins to list of account changes. Output first the inputs, then the outputs
    def extractReducedAccountChanges(txs: DataStream[ProcessedTx]):DataStream[ReducedAccountChange] = {
      txs.flatMap { tx =>
        val inputs = tx.inputs.zipWithIndex.map { case (entry, index) =>
          ReducedAccountChange(
            ts = tx.ts,
            height = tx.height,
            txPos = tx.txPos,
            value = 0L-entry.value,
            address = entry.address
          )
        }

        val outputs = tx.outputs.zipWithIndex.map { case (entry, index) =>
          ReducedAccountChange(
            ts = tx.ts,
            height = tx.height,
            txPos = tx.txPos,
            value = entry.value,
            address = entry.address
          )
        }
        inputs ++ outputs
      }
    }

    val reducedChanges:DataStream[ReducedAccountChange] = extractReducedAccountChanges(reducedTxs)
      .name("reduced-account-changes")
      .uid("reduced-account-changes")

    // Compute the transaction stacks
    // We will use the reduced transactions for those. It seems to be better than using the original inputs
    // and outputs directly


    val stackChanges = reducedChanges.keyBy(_.address)
      .flatMap(new TransactionStackFlatMap())
      .uid("transaction-stack-flatmap")
      .name("transaction-stack-processor")

    // Next we add a unique index for each record. This is useful in Kafka and clickhouse. To do that we need to sort
    // all rows in each transaction add add consecutive numbering to them. We do this with a window

    val outputStackChanges:DataStream[AccountModelChange] = stackChanges
      // We make tumbling windows of 100 ms. Since all txs in a block have a single timestamp, all of them will end
      // up in one window. The watermark we emit is +999 ms from the timestamp of the block, which means that when the
      // watermark is emitted, the window created for this block will be evaluated.
      //
      // Since windows are created only when a new element arrives, the small window size will not actually create a lot
      // of windows - There will be only one window per block. So we won't have problems with the resource usage.
      .windowAll(TumblingEventTimeWindows.of(Time.milliseconds(100)))
      .process[AccountModelChange](new IndexAccountChanges())

    stackChanges.print()
    //Compute token circulation
    //TODO


    //Send to the kafka sink
    ctx.consumeTransfers(accountChanges)
    ctx.consumeStackChanges(outputStackChanges)

    //processedTxs.print()

    ctx.env.execute("btc-block-processor")
  }
}

object Main extends BlockProcessor() {}

