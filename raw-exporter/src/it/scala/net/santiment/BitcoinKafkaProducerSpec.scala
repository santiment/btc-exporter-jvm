package net.santiment

import org.apache.kafka.clients.admin.NewTopic
import org.scalatest.FunSuite

import scala.collection.JavaConverters._

class BitcoinKafkaProducerSpec extends FunSuite {
  val testconf: Config = new Config {
    override lazy val kafkaTopic = "test-topic"
  }




  test ("lastProcessedBlockHeight should work when topic is empty") {
    val world = new Globals {
      override lazy val config:Config = testconf
    }
    world.adminClient.createTopics(Seq(new NewTopic("test-topic",1,1)).asJava)
    try {
      val producer = new BitcoinKafkaProducer(world)
      assert(producer.lastProcessedBlockHeight() == 0)
    } finally {
      world.adminClient.deleteTopics(Seq("test-topic").asJava)
    }
  }

  test ( "lastProcessedBlockHeigh should work when topic is not empty") {
    val world = new Globals {
      override lazy val config: Config = testconf
    }
    world.adminClient.createTopics(Seq(new NewTopic("test-topic", 1, 1)).asJava)
    try {
      world.sink.begin()
      world.sink.send("1", Array(1, 2, 3))
      world.sink.commit()
      world.sink.begin()
      world.sink.send("2", Array(1, 2, 4))
      world.sink.commit()
      world.sink.flush()
      //Thread.sleep(10000)
      val producer = new BitcoinKafkaProducer(world)
      assert(producer.lastProcessedBlockHeight() == 2)
    } finally {
      world.adminClient.deleteTopics(Seq("test-topic").asJava)
    }
  }

}
