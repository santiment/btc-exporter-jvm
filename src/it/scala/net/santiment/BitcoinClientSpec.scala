package net.santiment

import org.bitcoinj.core.Sha256Hash
import org.scalatest.FunSuite

import Globals._

class BitcoinClientSpec extends FunSuite {

  test("getBlockHash should work") {
    val hash = bitcoin.getBlockHash(500000)
    assert(hash == Sha256Hash.wrap("00000000000000000024fb37364cbf81fd49cc2d51c09c75c35433c3a1945d04"))
  }

  test("getBlock should work") {
    val block = bitcoin.getBlock(500000)
    assert(block.getNonce == 1560058197)
  }

  test("getTx should work") {
    val tx = bitcoin.getTx(Sha256Hash.wrap("2157b554dcfda405233906e461ee593875ae4b1b97615872db6a25130ecc1dd6"))
    assert(tx.getInput(0).getSequenceNumber == 4294967295L)
  }

  test("blockCount should work") {
    assert(bitcoin.blockCount > 10000)
  }
}
