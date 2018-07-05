package net

package object santiment {

  type BitcoinClientConfig = {
    val host: String
    val port: String
    val username: String
    val password: String
  }

}
