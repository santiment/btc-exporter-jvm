package net.santiment.btc.blockprocessor

import org.apache.flink.api.common.functions.Partitioner

class AddressPartitioner extends Partitioner[String] {
  override def partition(key: String, numPartitions: Int): Int = {
    if(numPartitions != 3) 0
    else key.hashCode % 3
  }
}
