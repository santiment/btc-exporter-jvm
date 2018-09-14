package net.santiment.btc

import com.typesafe.scalalogging.LazyLogging
import net.santiment.btc.blockprocessor.{BlockProcessorFlatMap, Globals, RawBlock}
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._

import scala.language.reflectiveCalls

class BlockProcessor
(
  context: {
    val env: StreamExecutionEnvironment
    val rawBlockSource: DataStream[RawBlock]
  }
)
  extends LazyLogging
{
  def main(args: Array[String]): Unit = {

    val processed = context.rawBlockSource
      .keyBy(_ =>null)
      .flatMap(new BlockProcessorFlatMap())
    processed.print()
    context.env.execute("btc-block-processor")
  }
}

object App extends BlockProcessor(Globals) {}

