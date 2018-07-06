import Dependencies._

enablePlugins(JavaAppPackaging)
enablePlugins(DockerPlugin)

lazy val root = (project in file("."))
  .enablePlugins(BuildInfoPlugin)
  .configs(IntegrationTest)
  .settings(
    Defaults.itSettings,

    inThisBuild(List(
      organization := "net.santiment",
      scalaVersion := "2.12.6",
      version      := "0.1.0-SNAPSHOT"
    )),

    name := "btc-exporter",

    resolvers += Resolver.bintrayRepo("msgilligan","maven"),

    libraryDependencies ++= Seq (
      scalaTest % s"${Test.name},${IntegrationTest.name}",
      scalaLogging,
      jsonrpc,
      bitcoinj,
      zookeeper
    ) ++ logging ++ curatorLibs,

    buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, sbtVersion),

    buildInfoPackage := organization.value
  )
