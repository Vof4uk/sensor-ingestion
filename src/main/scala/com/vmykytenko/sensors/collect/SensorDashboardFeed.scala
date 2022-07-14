package com.vmykytenko.sensors.collect

import org.apache.spark.sql.functions.{col, first, max_by}
import org.apache.spark.sql.types.BinaryType
import org.apache.spark.sql.{Encoders, SparkSession}

import scala.concurrent.duration.Duration

class SensorDashboardFeed(val backupInterval: Duration) {

  /**
   *
   * @param kafkaConsumerOptions - map of options to serve input messages from Kafka. Must contain keys:
   *                     "subscribe" - A comma-separated list of topics;
   *                     "kafka.bootstrap.servers" - A comma-separated list of host:port
   *                     and may contain a bunch of optional, for details see
   *                     https://spark.apache.org/docs/3.3.0/structured-streaming-kafka-integration.html#creating-a-kafka-source-for-batch-queries
   */
  def apply(servers: String,
            topic: String)(implicit spark: SparkSession): Unit = {
    println(servers)
    println(topic)
    val rawKafkaMessages = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", servers)
      .option("subscribe", topic)
      .load()

    implicit val encoder = Encoders.product[SensorMessage]

    rawKafkaMessages
      .select(
        col("topic"),
        col("key"),
        col("value"),
        col("timestamp"))
      .withWatermark("timestamp", "10 minutes")
      .groupBy(col("key"))
      .agg(
        col("key"),
        first(col("topic")),
        max_by(col("value"), col("timestamp")).as("value"))
      .select(col("value").cast(BinaryType))
      .map(row => {
        val topic = row.getAs[String]("topic")
        val bytes = row.getAs[Array[Byte]]("value")
        SensorMessageDeserializer.deserialize(topic, bytes)
      })
      .writeStream
      .option("checkpointLocation", "./checkpoints/")
//      .format("parquet")
//      .outputMode("update")
      .toTable("sensor_view")

  }
}

