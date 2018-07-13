package net.santiment

import org.bitcoinj.core.Utils
import org.bitcoinj.script.Script
import org.scalatest.FunSuite

/**
  * Unit tests for BitcoinClient
  */
class BitcoinClientSpec extends FunSuite {

  test("Testing P2PKH output") {
    //This is from output 0 from tx 0aa3d7cbbd35484632645675f5a6f28440a604975ce254c5701e4602f7d8dcc6
    val script:Script = new Script(Utils.HEX.decode("76a9148a31af61d6384177c4997c728948d5e690ff153488ac"))
    val address = BitcoinClient.extractAddress(script)
    assert(address.address == "1DbhmesUsNZ1hgX7KQtASNU9YRoqk8SiFZ")
    assert(address.kind == "P2PKH")
  }

  test("Testing P2PK output") {
    //Taken from tx 6880c2ae2f417bf6451029b716959ed255f127eeba3d7b40cf0ff3c44c5980c2
    val script:Script = new Script(Utils.HEX.decode("21030e7061b9fb18571cf2441b2a7ee2419933ddaa423bc178672cd11e87911616d1ac"))
    val address = BitcoinClient.extractAddress(script)
    assert(address.address == "1CjRf1RMrTwyGoBHDbqzXERhVFkPyowt8i")
    assert(address.kind == "P2PK")
  }

  test("Testing P2SH output") {
    //Taken from output 14 from tx 0aa3d7cbbd35484632645675f5a6f28440a604975ce254c5701e4602f7d8dcc6
    val script:Script = new Script(Utils.HEX.decode("a9146a00bbd84a3256df3df66cc2c69123c74f8a598e87"))
    val address = BitcoinClient.extractAddress(script)
    assert(address.address == "3BMWRrtUmsPaknqVj9jiihYxiPMWcEY8ae")
    assert(address.kind == "P2SH")
  }

  test("Testing support for SegWit addresses, P2WPKH") {
    //Taken from output 0 from tx 4ef47f6eb681d5d9fa2f7e16336cd629303c635e8da51e425b76088be9c8744c
    val script: Script = new Script(Utils.HEX.decode("0014e8df018c7e326cc253faac7e46cdc51e68542c42"))
    val address = BitcoinClient.extractAddress(script)
    assert(address.address == "bc1qar0srrr7xfkvy5l643lydnw9re59gtzzwf5mdq")
    assert(address.kind == "P2WPKH")
  }

  test("Testing support for SegWit address, P2WSH") {
    //Taken from output 1 from tx 4ef47f6eb681d5d9fa2f7e16336cd629303c635e8da51e425b76088be9c8744c
    val script: Script = new Script(Utils.HEX.decode("0020c7a1f1a4d6b4c1802a59631966a18359de779e8a6a65973735a3ccdfdabc407d"))
    val address = BitcoinClient.extractAddress(script)
    assert(address.address == "bc1qc7slrfxkknqcq2jevvvkdgvrt8080852dfjewde450xdlk4ugp7szw5tk9")
    assert(address.kind == "P2WSH")
  }
}
