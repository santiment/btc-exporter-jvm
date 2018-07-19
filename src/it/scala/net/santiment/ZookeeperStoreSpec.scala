package net.santiment

import com.typesafe.scalalogging.LazyLogging
import org.scalatest.{FunSuite, Outcome}
import org.scalatest.concurrent.TimeLimitedTests
import org.scalatest.time.Span
import org.scalatest.time.SpanSugar._

import Globals._
import Store.IntSerde

class ZookeeperStoreSpec
  extends FunSuite
    with TimeLimitedTests
    with LazyLogging{

  override val timeLimit: Span = 30 seconds

  def withStore(test: Store[Int] =>Any): Any = {
    logger.info(s"Initializing ${timeLimit}")
    val store = new ZookeeperStore[Int](zk, "/test/node")
    logger.info("Initialized")
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
