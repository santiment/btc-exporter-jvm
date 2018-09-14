import Dependencies._

import sbtassembly.AssemblyPlugin.defaultUniversalScript


ThisBuild / organization := "net.santiment"
ThisBuild / scalaVersion := SCALA_VERSION
ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / resolvers  ++=Seq(
  "Apache Development Snapshot Repository" at "https://repository.apache.org/content/repositories/snapshots/",
  Resolver.mavenLocal

)


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

    assemblyJarName in assembly := "raw-exporter.jar",

    assemblyOption in assembly := (assemblyOption in assembly).value.copy(prependShellScript = Some(defaultUniversalScript(shebang = true)))
)

lazy val blockprocessor = (project in file("block-processor"))
  .enablePlugins(BuildInfoPlugin)
  .configs(IntegrationTest)
  .settings(
    Defaults.itSettings,
    name := "block-processor",

    libraryDependencies ++= flinkDependencies
      ++ logging
      ++ Seq(
      scalaTest % s"${Test.name},${IntegrationTest.name}",
      bitcoinj
    ),

    scalacOptions ++= Seq("-target:jvm-1.8", "-unchecked", "-deprecation", "-feature"),

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




