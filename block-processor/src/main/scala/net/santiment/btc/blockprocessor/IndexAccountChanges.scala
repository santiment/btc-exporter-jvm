/*
package net.santiment.btc.blockprocessor

import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.bitcoinj.core.Coin

class IndexAccountChanges
extends ProcessAllWindowFunction[InternalAccountModelChange, AccountModelChange, TimeWindow] {
  override def process(context: Context, elements: Iterable[InternalAccountModelChange], out: Collector[AccountModelChange]): Unit = {
    elements.toArray
      .sortWith { case (early,late) =>
        if (early.height != late.height) {
          throw new IllegalStateException("Window contains txs from different blocks")
        }

        if (early.txPos != late.txPos) {
          early.txPos < late.txPos
        } else if (early.sign != late.sign) {
          early.sign < late.sign
        } else if (early.address != late.address) {
          early.address < late.address
        } else if (early.oheight != late.oheight) {
          (early.oheight < late.oheight) ^ (early.sign < 1)
        } else {
          true
        }
      }
      .zipWithIndex.foreach { case (internal, index) =>
        out.collect(AccountModelChange(
          sign = internal.sign,
          ts = internal.ts,
          height = internal.height,
          txPos = internal.txPos,
          oheight = internal.oheight,
          otxPos = internal.otxPos,
          ots = internal.ots,
          address = internal.address,
          value = Coin.valueOf(internal.value).toPlainString.toDouble
        ))
    }
  }
}
*/
