package net.santiment.btc

import com.typesafe.scalalogging.LazyLogging
import net.santiment.btc.blockprocessor.{AccountChange, BlockProcessorFlatMap, Globals, RawBlock}
import net.santiment.util.Migrator
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark

import scala.language.reflectiveCalls

class BlockProcessor
(
  context: {
    val migrator: Migrator
    val env: StreamExecutionEnvironment
    val rawBlockSource: DataStream[RawBlock]
    val consumeTransfers: DataStream[AccountChange]=>Unit
  }
)
  extends LazyLogging
{
  def main(args: Array[String]): Unit = {

    //Create kafka topic for storing btc transfers if not already created
    context.migrator.up()

    //This job is not parallel
    context.env.setParallelism(1)

    val processed = context.rawBlockSource
      // Dummy key which allows us to use keyed map state
      .keyBy(_ =>())

      // Extract account changes
      .flatMap(new BlockProcessorFlatMap())
      .uid("block-processor-flatmap")

      // Assign timestamps and watermarks. The timestamps will be recorded in the kafka topic
      .assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks[AccountChange] {
          override def checkAndGetNextWatermark(lastElement: AccountChange, extractedTimestamp: Long): Watermark = {
            // Generate watermark at the first entry from each block
            if(lastElement.txPos == 0 && lastElement.index == 0)
              // This is the first output of the coinbase transaction (which has no inputs)
              new Watermark(extractedTimestamp - 1)
            else
              null
          }

          override def extractTimestamp(element: AccountChange, previousElementTimestamp: Long): Long = {
            //timestamps are given in seconds so we need to convert to ms
            element.ts * 1000
          }
        })

    //Send to the kafka sink
    context.consumeTransfers(processed)

    //processed.print()
    context.env.execute("btc-block-processor")
  }
}

object App extends BlockProcessor(Globals) {}

