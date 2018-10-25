import Dependencies._

import sbtassembly.AssemblyPlugin.defaultUniversalScript


ThisBuild / organization := "net.santiment"
ThisBuild / scalaVersion := SCALA_VERSION
ThisBuild / version := "1"

ThisBuild / resolvers  ++=Seq(
  "Apache Development Snapshot Repository" at "https://repository.apache.org/content/repositories/snapshots/",
  Resolver.mavenLocal

)

ThisBuild / fork := true

cancelable in Global := true


lazy val oldexporter = (project in file("old-exporter"))
  .dependsOn(util)
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
      kafkaClients
    ) ++ logging ++ curatorLibs ++ jackson,

    buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, sbtVersion),

    buildInfoPackage := "net.santiment.btc.oldexporter",

    //Don't run tests during assembly
    test in assembly := {},

    assemblyJarName in assembly := name.value.concat(".jar"),

    assemblyOption in assembly := (assemblyOption in assembly).value.copy(prependShellScript = Some(defaultUniversalScript(shebang = true)))
  )

lazy val util = (project in file("util"))
  .enablePlugins(BuildInfoPlugin)
  .configs(IntegrationTest)
  .settings(
    Defaults.itSettings,

    name := "btc-util",

    buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, sbtVersion),

    buildInfoPackage := "net.santiment.btc.util",

    //Don't run tests during assembly
    test in assembly := {},

    libraryDependencies ++= Seq (
      scalaLogging,
      kafkaClients,
      zookeeper
    ) ++ curatorLibs
  )

lazy val rawexporter = (project in file("raw-exporter"))
  .dependsOn(util)
  .enablePlugins(BuildInfoPlugin)
  .configs(IntegrationTest)
  .settings(
    Defaults.itSettings,

    name := "btc-raw-exporter",

    resolvers += Resolver.bintrayRepo("msgilligan","maven"),

    libraryDependencies ++= Seq (
      scalaTest % s"${Test.name},${IntegrationTest.name}",
      scalaLogging,
      jsonrpc,
      bitcoinj,
      zookeeper,
      kafkaClients
    ) ++ logging ++ curatorLibs ++ jackson,

    buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, sbtVersion),

    buildInfoPackage := "net.santiment.btc.rawexporter",

    //Don't run tests during assembly
    test in assembly := {},

    assemblyJarName in assembly := "raw-exporter.jar",

    assemblyOption in assembly := (assemblyOption in assembly).value.copy(prependShellScript = Some(defaultUniversalScript(shebang = true)))
)

lazy val blockprocessor = (project in file("block-processor"))
  .dependsOn(util)
  .enablePlugins(BuildInfoPlugin)
  .configs(IntegrationTest)
  .settings(
    Defaults.itSettings,
    name := "block-processor",

    version := "1.2.0",

    libraryDependencies ++= flinkDependencies
      ++ logging
      ++ jackson
      ++ Seq(
      scalaTest % s"${Test.name},${IntegrationTest.name}",
      kafka % s"${Test.name},${IntegrationTest.name}",
      bitcoinj
    ),

    scalacOptions ++= Seq("-target:jvm-1.8", "-unchecked", "-deprecation", "-feature"),

    buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, sbtVersion),

    buildInfoPackage := "net.santiment.btc.blockprocessor",


    //exclude Scala library from assembly
    assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false),

    assemblyJarName in assembly := "block-processor.jar",

    assemblyMergeStrategy in assembly := {
      case PathList("META-INF", xs @ _*) => MergeStrategy.discard
      case _ => MergeStrategy.first
    },

    // make run command include the provided dependencies
    run in Compile := Defaults.runTask(fullClasspath in Compile,
      mainClass in (Compile, run),
      runner in (Compile,run)).evaluated
  )

