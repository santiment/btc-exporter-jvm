package net.santiment

import com.typesafe.scalalogging.LazyLogging
import org.scalatest.{FunSuite, Outcome}
import org.scalatest.concurrent.TimeLimitedTests
import org.scalatest.time.Span
import org.scalatest.time.SpanSugar._

import Globals._

class ZookeeperStoreSpec
  extends FunSuite
    with TimeLimitedTests
    with LazyLogging{

  val timeLimit: Span = 30 seconds

  def withStore(test: Store[Integer] =>Any): Any = {
    val store = new ZookeeperStore[Integer](zk, "/test/node")

    try {
      test(store)
    } finally {
      zk.delete().deletingChildrenIfNeeded().forPath("/")

    }
  }

  test ("It should work") {
    withStore { store =>
      logger.info("Creating")
      store.create(10)
      logger.info("Getting")
      val result = store.read.get
      logger.info("Deleting")
      assert(result == 10)

    }
  }

}
