ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.16"

lazy val root = (project in file("."))
  .settings(
    name := "sensor-ingestion"
  )

libraryDependencies += "org.apache.spark" % "spark-sql_2.12" % "3.3.0" % Provided
libraryDependencies += "org.apache.spark" % "spark-streaming_2.12" % "3.3.0" % Provided
libraryDependencies += "org.apache.spark" % "spark-streaming-kafka-0-10_2.12" % "3.3.0" % Provided

libraryDependencies += "org.testcontainers" % "testcontainers" % "1.17.3" % Test
libraryDependencies += "org.testcontainers" % "kafka" % "1.17.3" % Test
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.12" % Test
libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.3.0" % Test
libraryDependencies += "org.apache.spark" % "spark-sql_2.12" % "3.3.0" % Test
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "2.8.1" % Test
libraryDependencies += "org.apache.spark" % "spark-streaming_2.12" % "3.3.0" % Test
libraryDependencies += "org.apache.spark" % "spark-streaming-kafka-0-10_2.12" % "3.3.0" % Test


