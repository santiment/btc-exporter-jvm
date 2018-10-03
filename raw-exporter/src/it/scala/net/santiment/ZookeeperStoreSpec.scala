package net.santiment

import com.typesafe.scalalogging.LazyLogging
import net.santiment.Globals._
import net.santiment.util.Store.IntSerde
import net.santiment.util.{Store, ZookeeperStore}
import org.scalatest.FunSuite
import org.scalatest.concurrent.TimeLimitedTests
import org.scalatest.time.Span
import org.scalatest.time.SpanSugar._

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
