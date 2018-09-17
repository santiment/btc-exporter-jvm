package net.santiment.btc

import com.typesafe.scalalogging.LazyLogging
import net.santiment.btc.blockprocessor.{AccountChange, BlockProcessorFlatMap, Globals, RawBlock}
import net.santiment.util.Migrator
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala._

import scala.language.reflectiveCalls

class BlockProcessor
(
  context: {
    val migrator: Migrator
    val env: StreamExecutionEnvironment
    val rawBlockSource: DataStream[RawBlock]
    val transfersSink: SinkFunction[AccountChange]
  }
)
  extends LazyLogging
{
  def main(args: Array[String]): Unit = {

    context.migrator.up()

    //This job is not parallel
    context.env.setParallelism(1)

    val processed = context.rawBlockSource
      .keyBy(_ =>())
      .flatMap(new BlockProcessorFlatMap())
      .uid("block-processor-flatmap")

    processed.addSink(context.transfersSink).uid("transfers-kafka-sink")

    //processed.print()
    context.env.execute("btc-block-processor")
  }
}

object App extends BlockProcessor(Globals) {}

