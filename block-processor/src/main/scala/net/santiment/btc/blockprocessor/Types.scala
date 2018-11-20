package net.santiment.btc.blockprocessor

import org.bitcoinj.core.{Coin, TransactionOutPoint, TransactionOutput}
import org.bitcoinj.script.Script

case class RawBlock(height:Int, bytes: ByteArray)

/**
  * Represents an account change record intended for storing in Clickhouse
  * and for calculating simple metrics.
  *
  * @param in -- is this an input or an output (true = input, false = output)
  * @param ts -- block timestamp in miliseconds
  * @param height -- block height
  * @param txPos -- position of originating transaction in the block
  * @param value -- amount of row in BTC. It is negative for inputs and positive for outputs
  * @param address -- address affected by this change
  *
  * The combination of (height, txPos, address) is an unique key
  */
case class AccountChange
(
  in: Boolean,
  ts: Long,
  height: Int,
  txPos: Int,
  value: Double,
  address: String
)

/**
  * This is similar to the [[AccountChange]], however it is intended for internal use.
  * Here the value is given in satoshis. and we record inputs with negative value, and outputs with positive.
  *
  * @param ts
  * @param height
  * @param txPos
  * @param value
  * @param address
  */
case class ReducedAccountChange
(
  ts: Long,
  height: Int,
  txPos: Int,
  value: Long,
  address: String
)

/**
  * Represents an entry in the input or output list of a processed transaction.
  * @param address -- address of the input/output
  * @param value -- value of the entry in satoshis. Always positive
  */
case class TxEntry
(
  address: String,
  value: Long
)

/**
  * Processed transaction. This is the result of parsing the transactions from the raw blocks and fetching all input
  * values
  * @param ts -- transaction timestamp
  * @param height -- height of the block which contains the transaction
  * @param txPos -- position of this transaction in the list of transactions for the containing block
  * @param inputs -- list of inputs ordered as in the raw block
  * @param outputs -- list of outputs ordered as in the raw block
  */
case class ProcessedTx
(
  ts: Long,
  height: Int,
  txPos: Int,
  inputs: Array[TxEntry],
  outputs: Array[TxEntry]
)

/**
  * Account moodel changes formatted in a way suitable for consumption by Kafka and Clickhouse
  * @param sign    - +1 for added records, -1 for deleted records. This convention is used in Clickhouse's
      CollapsingMergeTree, so hopefully we can use Clickhouse for some metrics based on transaction stacks.
  * @param ts
  * @param height
  * @param txPos
  * @param nonce
  * @param ots
  * @param oheight
  * @param otxPos
  * @param address
  * @param value
  */
case class AccountModelChange
(
  // This should be +1 for added records and -1 for deleted records. This convention is used in Clickhouse's
  // CollapsingMergeTree, so hopefully we can use Clickhouse for some metrics based on the account model.
  sign: Int,

  //Reference to the current transaction at which this record is produced (equivalent to $n$ in the documentation)
  ts: Long,
  height: Int,
  txPos: Int,

  // nonce is used for disambiguation. The triple (sign, address, nonce) is a unique key for each account model
  // change
  nonce: Int,

  // Reference for the originating transaction for this segment (denoted by $Tx(s)$ in the documentation). We only need
  // the timestamp, so the original height and otxPos values will be skipped. This saves 8 bytes per record
  ots: Long,
  //oheight: Int,
  //otxPos: Int,

  address: String,

  // Value of the segment. It should always be positive
  value: Double
)

/**
  * Used for internal representation of account model changes. Similar to [[AccountModelChange]], however
  * here we don't have an index, because no need to create a unique key for this record. Also the value is
  * recorded with an integer instead of double.
  * @param sign
  * @param ts
  * @param height
  * @param txPos
  * @param ots
  * @param oheight
  * @param otxPos
  * @param address
  * @param value
  */
case class InternalAccountModelChange
(
  sign: Int,
  ts: Long,
  height: Int,
  txPos: Int,
  nonce: Int,
  ots: Long,
  //oheight: Int,
  //otxPos: Int,
  address: String,
  value: Long
)

trait Address[T] {
  def address(value:T):String
}

/**
  * Account model segment. [[ots]], [[oheight]] and [[otxPos]] reference for the originating transaction for this
  * segment (denoted by $Tx(s)$ in the documentation)
  * @param ots
  * @param oheight
  * @param otxPos
  * @param value Value of the segment. Positive value means this segment is an asset. Negative value (which can happen
  *              in some special situations) means that this segment is a liability
  */
case class Segment
(
  // The nonce is different for each segment associated to a given address. So the tuple (address,nonce) is a unique
  // identifier for each segment
  nonce: Int,

  // Reference for the originating transaction for this segment (denoted by $Tx(s)$ in the documentation). We only need the
  // timestamp, so we won't put a reference to the originating transaction here. This saves 8 bytes
  ots: Long,
  //oheight: Int,
  //otxPos: Int,

  // In the documentation we require to also have the address as part of the segment. However in practice we store
  // the segments keyed by the address, so no need to also keep it as part of the segment here. It would only take up
  // space.

  // address: String,

  // Value of the segment. Positive value means this segment is an asset. Negative value (which can happen in some
  // special situations) means that this segment is a liability
  value: Long
)

/**
  * One unprocessed tx entry, either input or output.
  * @param ts block timestamp
  * @param height block height
  * @param txPos tx position in block
  * @param key -  If output, this is contains the transaction hash of the output and the index . If input, this contains
  *            the data for the corresponding output
  * @param value - If input, this is null. If output this contains the script and tha bitcoin value of the output
  *
  * We are also using one special record: In this record the key is null. It indicates that there are no more records
  * for this block, so we can emit a watermark.
  */

case class UnmatchedTxEntry
(
ts: Long,
height: Int,
txPos: Int,
key: Option[OutputKey],
value: Option[Output]
)

/**
  *
  * @param hash a byte-array containing transaction hash converted to a string. We need a string here, because current flink
  *             version does not allow arrays in keys
  * @param index index of the output in its transaction
  */
case class OutputKey(_hash: String, index: Long) {
  def hash: Array[Byte] = _hash.toCharArray.map(_.toByte)
}

case class ParsedOutput(script:Script, value:Coin)

case class Output(script:ByteArray, value:Long) {
  def parse() : ParsedOutput  = ParsedOutput(new Script(script), Coin.valueOf(value))
}

object OutputKey {
  def fromOutpoint(o:TransactionOutPoint):OutputKey = {
    OutputKey(
      o.getHash.getBytes.map(_.toChar).mkString,
      o.getIndex)
  }

}

object Output {
  def fromTxOutput(out:TransactionOutput) : Output = Output(out.getScriptBytes, out.getValue.value)
}



object Types {
  implicit object AccountChangeAddress extends Address[AccountChange] {
    def address(value:AccountChange): String = value.address
  }

  implicit object AccountModelChangeAddress extends Address[AccountModelChange] {
    def address(value:AccountModelChange): String = value.address
  }



}