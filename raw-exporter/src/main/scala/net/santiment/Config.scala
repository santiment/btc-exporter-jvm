package net.santiment

case class BitcoinClientConfig
(
  host:String,
  port:String,
  username: String,
  password: String
)


case class ZookeeperConfig
(
  connectionString: String,
  namespace: String
)

case class KafkaConfig
(
  bootstrapServers: String
)
