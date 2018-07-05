package net.santiment

import org.scalatest.FunSuite

class ZookeeperCheckpointerSpec extends FunSuite {

  test ("It should work") {
    val store = new ZookeepeerCheckpointer[Int](Config.blockCheckpointConfig)
    store.create(10)
    val result = store.read.get
    assert(result == 10)
  }

}
