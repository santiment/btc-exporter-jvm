package net.santiment

object Config {
  lazy val bitcoind:BitcoinClientConfig = new {
    val host: String = sys.env.getOrElse("BITCOIND_URL", "localhost")
    val port: String = sys.env.getOrElse("BITCOIND_PORT", "8332")
    val username: String = sys.env("BITCOIND_USER")
    val password: String = sys.env("BITCOIND_PASSWORD")
  }
}
