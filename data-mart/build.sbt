ThisBuild / scalaVersion := "2.12.18"

name := "openfoodfacts-data-mart"
version := "0.1.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.1",
  "org.apache.spark" %% "spark-sql" % "3.5.1",
  "com.datastax.spark" %% "spark-cassandra-connector" % "3.5.1",
  "com.typesafe.play" %% "play-json" % "2.10.6"
)

Compile / run / fork := true

Compile / run / javaOptions ++= Seq(
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
)