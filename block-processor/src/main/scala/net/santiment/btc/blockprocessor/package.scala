package net.santiment.btc

package object blockprocessor {
  type ByteArray = Array[Byte]

  case class RawBlock(height:Int, bytes: ByteArray)

  /**
    * Represents an account change record intended for storing in Clickhouse
    * and for calculating simple metrics.
    *
    * @param in -- is this an input or an output (true = input, false = output)
    * @param ts -- block timestamp in miliseconds
    * @param height -- block height
    * @param txPos -- position of originating transaction in the block
    * @param index -- index of the input/output of the transaction.
    * @param value -- amount of row in BTC. It is negative for inputs and positive for outputs
    * @param address -- address affected by this change
    *
    * The combination of (in, height, txPos, index) is an unique key
    */
  case class AccountChange
  (
    in: Boolean,
    ts: Long,
    height: Int,
    txPos: Int,
    index: Int,
    value: Double,
    address: String
  )
}
