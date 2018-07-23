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
    val block = bitcoin.getBlock(500000)
    assert(block.getNonce == 1560058197)
  }

  test("getTx should work") {
    val tx = bitcoinClient.getTx(Sha256Hash.wrap("2157b554dcfda405233906e461ee593875ae4b1b97615872db6a25130ecc1dd6"))
    assert(tx.getInput(0).getSequenceNumber == 4294967295L)
  }

  test("blockCount should work") {
    assert(bitcoin.blockCount > 10000)
  }

  test("getTxList should work") {
    logger.debug("getTxList")

    val tx1 = Sha256Hash.wrap("2157b554dcfda405233906e461ee593875ae4b1b97615872db6a25130ecc1dd6")
    val tx2 = Sha256Hash.wrap("0aa3d7cbbd35484632645675f5a6f28440a604975ce254c5701e4602f7d8dcc6")
    val result = bitcoinClient.getTxList(Set(tx1,tx2))
    assert(result(tx1).getInput(0).getSequenceNumber == 4294967295L)
    assert(result(tx2).getInput(0).getSequenceNumber == 4294967294L)
  }

}
