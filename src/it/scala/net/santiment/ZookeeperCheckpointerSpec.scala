package net.santiment

import com.typesafe.scalalogging.LazyLogging
import org.scalatest.{FunSuite, Outcome}
import org.scalatest.concurrent.TimeLimitedTests
import org.scalatest.time.Span
import org.scalatest.time.SpanSugar._


class ZookeeperCheckpointerSpec
  extends FunSuite
    with TimeLimitedTests
    with LazyLogging{

  val timeLimit: Span = 5 seconds

  def withStore(test: Checkpointer[Integer] =>Any): Any = {
    val store = new ZookeeperCheckpointer[Integer](Config.blockCheckpointer)

    try {
      test(store)
    } finally {
      store.client.delete().deletingChildrenIfNeeded().forPath("/")
      store.close()
    }
  }

  test ("It should work") {
    withStore { store =>
      store.create(10)
      val result = store.read.get
      assert(result == 10)
    }
  }

}
