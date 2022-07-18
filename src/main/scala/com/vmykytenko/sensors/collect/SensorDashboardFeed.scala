package com.vmykytenko.sensors.collect

import com.vmykytenko.sensors.{SensorMessage, SensorMessageDe}
import org.apache.spark.sql.{Encoders, SparkSession}

case object SensorDashboardFeed {

  /**
   *
   * @param kafkaConsumerOptions - map of options to serve input messages from Kafka. Must contain keys:
   *                             "subscribe" - A comma-separated list of topics;
   *                             "kafka.bootstrap.servers" - A comma-separated list of host:port
   *                             and may contain a bunch of optional, for details see
   *                             https://spark.apache.org/docs/3.3.0/structured-streaming-kafka-integration.html#creating-a-kafka-source-for-batch-queries
   */
  def apply(servers: String,
            topic: String,
            storageOptions: Map[String, String],
            isTest: Boolean)(implicit spark: SparkSession): Unit = {

    val rawKafkaMessages = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", servers)
      .option("subscribe", topic)
      .option("startingOffsets", "earliest")
      .load()

    implicit val encoderIn = Encoders.product[SensorMessage]
    val parsedStream = rawKafkaMessages
      .map(row => {
        val topic = row.getAs[String]("topic")
        SensorMessageDe.deserialize(topic, row.getAs[Array[Byte]]("value"))
      })

    parsedStream
      .writeStream
      .partitionBy("environmentName", "deviceName", "timestamp")
      .format("parquet")
      .option("checkpointLocation", storageOptions("checkpoint.location"))
      .option("path", storageOptions("parquet.path"))
      .start()

    if (isTest) {
      rawKafkaMessages.writeStream.format("console").start()
      parsedStream.writeStream.format("console").start()
    }
  }
}