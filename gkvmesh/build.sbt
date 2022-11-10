val scala3Version = "3.2.0"

lazy val root = project
  .in(file("."))
  .settings(
    name := "gkvmesh",
    version := "0.1.0-SNAPSHOT",
    scalaVersion := scala3Version,
    libraryDependencies ++= Seq(
      "org.scalameta" %% "munit" % "0.7.29" % Test,
      "org.foundationdb" % "fdb-java" % "7.1.23",
      "ch.qos.logback" % "logback-classic" % "1.2.10",
      "com.typesafe.scala-logging" %% "scala-logging" % "3.9.4",
      "commons-codec" % "commons-codec" % "1.15",
      "io.grpc" % "grpc-netty" % "1.50.2",
      "com.thesamet.scalapb" %% "scalapb-runtime-grpc" % scalapb.compiler.Version.scalapbVersion,
      "com.thesamet.scalapb" %% "scalapb-json4s" % "0.12.0",
      "commons-net" % "commons-net" % "3.8.0"
    )
  )

Compile / PB.targets := Seq(
  scalapb.gen() -> (Compile / sourceManaged).value / "scalapb"
)

scalacOptions ++= Seq("-deprecation")

assembly / assemblyMergeStrategy := {
  case "module-info.class"                             => MergeStrategy.discard
  case x if x.contains("io.netty.versions.properties") => MergeStrategy.discard
  case x =>
    val oldStrategy = (assembly / assemblyMergeStrategy).value
    oldStrategy(x)
}
