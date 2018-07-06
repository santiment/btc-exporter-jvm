package net.santiment

case class BitcoinClientConfig
(
  host:String,
  port:String,
  username: String,
  password: String
)

case class ZookeeperCheckpointerConfig
(
  connectionString: String,
  namespace: String,
  path: String
)

case class KafkaConfig
(
  topic: String
)

case class Config
(
  bitcoind: BitcoinClientConfig,
  blockCheckpointer: ZookeeperCheckpointerConfig,
  kafka: KafkaConfig
)

object Config extends Config(
  bitcoind = BitcoinClientConfig(
    host = sys.env.getOrElse("BITCOIND_URL", "localhost"),
    port = sys.env.getOrElse("BITCOIND_PORT", "8332"),
    username = sys.env("BITCOIND_USER"),
    password = sys.env("BITCOIND_PASSWORD")
  ),

  kafka = KafkaConfig(
    topic = sys.env("KAFKA_TOPIC")
  ),

  blockCheckpointer = ZookeeperCheckpointerConfig(
    connectionString = sys.env.getOrElse("ZOOKEEPER_URL", "localhost:2181"),
    namespace = s"${BuildInfo.name}",
    path = s"/${sys.env("KAFKA_TOPIC")}/block-number"
  )
)
