package net.santiment

import org.bitcoinj.core.Sha256Hash
import org.scalatest.FunSuite


class BitcoinClientSpec extends FunSuite {

  test("getBlockHash should work") {
    val client = new BitcoinClient(Config.bitcoind)
    val hash = client.getBlockHash(500000)
    assert(hash == Sha256Hash.wrap("00000000000000000024fb37364cbf81fd49cc2d51c09c75c35433c3a1945d04"))
  }

  test("getBlock should work") {
    val client = new BitcoinClient(Config.bitcoind)
    val block = client.getBlock(500000)
    assert(block.getNonce == 1560058197)
  }

  test("getTx should work") {
    val client = new BitcoinClient(Config.bitcoind)
    val tx = client.getTx(Sha256Hash.wrap("2157b554dcfda405233906e461ee593875ae4b1b97615872db6a25130ecc1dd6"))
    assert(tx.getInput(0).getSequenceNumber == 4294967295L)
  }
}
