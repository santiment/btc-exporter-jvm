package net.santiment.btc.blockprocessor

import com.typesafe.scalalogging.LazyLogging
import net.santiment.util.Migrator
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.watermark.Watermark

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

    val processed = ctx.rawBlockSource
      // Dummy key which allows us to use keyed map state
      .keyBy(_ =>())

      // Extract account changes
      .flatMap(new BlockProcessorFlatMap())(implicitly[TypeInformation[AccountChange]])
      .uid("block-processor-flatmap")
      .name("block-processor")

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
    ctx.consumeTransfers(processed)

    //processed.print()
    ctx.env.execute("btc-block-processor")
  }
}

object Main extends BlockProcessor() {}

