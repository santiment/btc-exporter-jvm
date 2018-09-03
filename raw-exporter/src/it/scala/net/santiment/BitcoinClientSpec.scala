package net.santiment

import org.bitcoinj.core.{Sha256Hash, Utils}
import org.scalatest.FunSuite
import Globals._
import com.typesafe.scalalogging.LazyLogging
import org.bitcoinj.script.{Script, ScriptBuilder}

/**
  * Integration tests for BitcoinClient
  */
class BitcoinClientSpec extends FunSuite with LazyLogging {

  logger.debug("Starting test suite")

  test("getBlockHash should work") {
    val hash = bitcoinClient.getBlockHash(500000)
    assert(hash == Sha256Hash.wrap("00000000000000000024fb37364cbf81fd49cc2d51c09c75c35433c3a1945d04"))
  }

  test("getBlock should work") {
    val block = bitcoinClient.getBlock(500000)
    assert(block.getNonce == 1560058197)
  }

  test("blockCount should work") {
    assert(bitcoinClient.blockCount > 10000)
  }
}
