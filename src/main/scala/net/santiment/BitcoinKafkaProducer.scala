package net.santiment

object BitcoinKafkaProducer extends App {
  val client = new BitcoinClient(Config.bitcoind)
}
