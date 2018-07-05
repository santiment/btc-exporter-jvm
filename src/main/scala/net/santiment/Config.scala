package net.santiment

object Config {
  lazy val bitcoind:BitcoinClientConfig = new {
    val host: String = sys.env.getOrElse("BITCOIND_URL", "localhost")
    val port: String = sys.env.getOrElse("BITCOIND_PORT", "8332")
    val username: String = sys.env("BITCOIND_USER")
    val password: String = sys.env("BITCOIND_PASSWORD")
  }

  lazy val blockCheckpointConfig: ZookeeperCheckpointerConfig = new {
    val namespace: String = s"/${BuildInfo.name}"
    val path: String = s"/${Config.kafkaConfig.topic}/block-number"
    val connectionString: String = sys.env.getOrElse("ZOOKEEPER_URL", "localhost:2181")
  }

  lazy val kafkaConfig = new {
    val topic:String = sys.env("KAFKA_TOPIC")
  }
}
