import sbt._

object Dependencies {
  lazy val KAFKA_VERSION = "1.1.0" //sys.env("KAFKA_VERSION")
  lazy val ZOOKEEPER_VERSION = "3.4.12" //sys.env("ZOOKEEPER_VERSION")

  lazy val scalaTest = "org.scalatest" %% "scalatest" % "3.0.5"

  lazy val kafka = ("org.apache.kafka" % "kafka-clients" % KAFKA_VERSION)
    .exclude("log4j", "log4j")
    .exclude("org.slf4j", "slf4j-log4j12")

  lazy val jsonrpc = "com.github.briandilley.jsonrpc4j" % "jsonrpc4j" % "1.5.3"

  lazy val bitcoinj = "org.bitcoinj" % "bitcoinj-core" % "0.14.7"

  lazy val zookeeper = ("org.apache.zookeeper" % "zookeeper" % ZOOKEEPER_VERSION)
    .exclude("log4j", "log4j")
    .exclude("org.slf4j", "slf4j-log4j12")


  /**
   * There can be more curator libs in future.
   * We exclude zookeeper so that we can support 3.4.x, as described in the Curator
   * docs: http://curator.apache.org/zk-compatibility.html
  */
  lazy val curatorLibs = Seq("curator-framework").map { x =>
    ("org.apache.curator" % x % "4.0.1")
      .exclude("org.apache.zookeeper", "zookeeper")
  }

  lazy val logback: Seq[ModuleID] = Seq("logback-classic", "logback-core").map { x =>
    "ch.qos.logback" % x % "1.2.3"
  }

  lazy val scalaLogging = "com.typesafe.scala-logging" %% "scala-logging" % "3.8.0"

  lazy val log4jOverSlf4j =  "org.slf4j" % "log4j-over-slf4j" % "1.7.25"

  lazy val logging = logback ++ Seq(scalaLogging)

}
