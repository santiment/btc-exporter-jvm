import Dependencies._

import sbtassembly.AssemblyPlugin.defaultUniversalScript

organization := "net.santiment"
scalaVersion := "2.12.6"
version := "0.1.0-SNAPSHOT"

lazy val oldexporter = (project in file("old-exporter"))
  .enablePlugins(BuildInfoPlugin)
  .configs(IntegrationTest)
  .settings(
    Defaults.itSettings,

    name := "btc-exporter",

    resolvers += Resolver.bintrayRepo("msgilligan","maven"),

    libraryDependencies ++= Seq (
      scalaTest % s"${Test.name},${IntegrationTest.name}",
      scalaLogging,
      jsonrpc,
      bitcoinj,
      zookeeper,
      kafka
    ) ++ logging ++ curatorLibs ++ jackson,

    buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, sbtVersion),

    buildInfoPackage := organization.value,

    //Don't run tests during assembly
    test in assembly := {},

    assemblyJarName in assembly := name.value.concat(".jar"),

    assemblyOption in assembly := (assemblyOption in assembly).value.copy(prependShellScript = Some(defaultUniversalScript(shebang = true)))
)

lazy val rawexporter = (project in file("raw-exporter"))
  .enablePlugins(BuildInfoPlugin)
  .configs(IntegrationTest)
  .settings(
    Defaults.itSettings,

    inThisBuild(List(
      organization := "net.santiment",
      scalaVersion := "2.12.6",
      version      := "0.1.0-SNAPSHOT"
    )),

    name := "btc-raw-exporter",

    resolvers += Resolver.bintrayRepo("msgilligan","maven"),

    libraryDependencies ++= Seq (
      scalaTest % s"${Test.name},${IntegrationTest.name}",
      scalaLogging,
      jsonrpc,
      bitcoinj,
      zookeeper,
      kafka
    ) ++ logging ++ curatorLibs ++ jackson,

    buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, sbtVersion),

    buildInfoPackage := organization.value,

    //Don't run tests during assembly
    test in assembly := {},

    assemblyJarName in assembly := name.value.concat(".jar"),

    assemblyOption in assembly := (assemblyOption in assembly).value.copy(prependShellScript = Some(defaultUniversalScript(shebang = true)))
)

