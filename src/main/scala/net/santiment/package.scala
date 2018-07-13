package net
import org.bitcoinj.core.Coin

package object santiment {

  case class TransactionEntry(account: BitcoinAddress, value: Coin)

  case class ResultTx
  (
    from: String,
    to: String,
    value: Double,
    blockNumber: Int,
    timestamp: Long,
    transactionHash: String
  )


}
