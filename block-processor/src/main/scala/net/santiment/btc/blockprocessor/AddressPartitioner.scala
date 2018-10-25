package net.santiment.btc.blockprocessor

import org.apache.flink.api.common.functions.Partitioner
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner

class AddressPartitioner(val numTopics: Int) extends Partitioner[String] {
  override def partition(key: String, numPartitions: Int): Int = {
    Math.floorMod(Math.floorDiv(key.hashCode, numTopics),numPartitions)
  }
}

class KafkaPartitioner[T](val numTopics: Int, keyGetter: T=>String) extends FlinkKafkaPartitioner[T] {
  override def partition(record: T, key: Array[Byte], value: Array[Byte], targetTopic: String, partitions: Array[Int]): Int = {
    val sorted = partitions.sorted
    val key = keyGetter(record)
    sorted(Math.floorMod(Math.floorDiv(key.hashCode, numTopics),sorted.length))
  }
}