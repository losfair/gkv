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
      "org.rocksdb" % "rocksdbjni" % "7.7.3"
    )
  )
