import sbt._

object Dependencies {
  lazy val KAFKA_VERSION = sys.env.get("KAFKA_VERSION").get
  lazy val ZOOKEEPER_VERSION = sys.env.get("ZOOKEEPER_VERSION").get
  lazy val scalaTest = "org.scalatest" %% "scalatest" % "3.0.5"
  lazy val kafka = "org.apache.kafka" %% "kafka-clients" % KAFKA_VERSION
  lazy val zookeeper = "org.apache.zookeeper" % "zookeeper" % ZOOKEEPER_VERSION pomOnly()
  lazy val jsonrpc = "com.github.briandilley.jsonrpc4j" % "jsonrpc4j" % "1.5.3"
  lazy val bitcoinj = "org.bitcoinj" % "bitcoinj-core" % "0.14.7"

  lazy val logback = Seq("logback-classic", "logback-core").map { x =>
    "ch.qos.logback" % x % "1.2.3"
  }
}
