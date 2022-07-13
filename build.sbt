ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.16"

lazy val root = (project in file("."))
  .settings(
    name := "sensor-ingestion"
  )

libraryDependencies += "org.apache.spark" % "spark-streaming_2.12" % "3.3.0" % "provided"
libraryDependencies += "org.apache.spark" % "spark-streaming-kafka-0-10_2.12" % "3.3.0" % "provided"

libraryDependencies += "org.testcontainers" % "testcontainers" % "1.17.3" % "test"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.12" % "test"