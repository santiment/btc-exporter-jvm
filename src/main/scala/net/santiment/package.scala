package net

package object santiment {

  type BitcoinClientConfig = {
    val host: String
    val port: String
    val username: String
    val password: String
  }

  type ZookeeperCheckpointerConfig = {
    val namespace: String
    val path: String
    val connectionString: String
  }

}
