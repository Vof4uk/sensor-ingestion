package com.vmykytenko.sensors.collect

import com.vmykytenko.sensors.{KafkaConsumerConfig, SensorMessage, SensorMessageDe}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.{Encoders, SparkSession}

case object SensorDataFeed {

  /**
   *
   * @param kafkaConsumerOptions - map of options to serve input messages from Kafka. Must contain keys:
   *                             "subscribe" - A comma-separated list of topics;
   *                             "kafka.bootstrap.servers" - A comma-separated list of host:port
   *                             and may contain a bunch of optional, for details see
   *                             https://spark.apache.org/docs/3.3.0/structured-streaming-kafka-integration.html#creating-a-kafka-source-for-batch-queries
   */
  def apply(consumerConfig: KafkaConsumerConfig,
            storageOptions: Map[String, String],
            isTest: Boolean)(implicit spark: SparkSession): Unit = {

    val rawKafkaMessages = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", consumerConfig.servers)
      .option("subscribe", consumerConfig.topic)
      .option("startingOffsets", "earliest")
      .load()

    implicit val encoderIn = Encoders.product[SensorMessage]
    val parsedStream = rawKafkaMessages
      .map(row => {
        val topic = row.getAs[String]("topic")
        SensorMessageDe.deserialize(topic, row.getAs[Array[Byte]]("value"))
      })

    val windowSize = "5 minutes"
    val grouped = parsedStream
      .withColumn("watermark", timestamp_seconds(col("timestamp")))
      .withWatermark("watermark", windowSize)
      .groupBy(
        window(col("watermark"), windowSize),
        col("environmentName"),
        col("deviceName"),
        col("metric")
      )
      .agg(
        max_by(col("value"), col("timestamp")).as("value"),
        max("timestamp").as("timestamp")
      )

    grouped
      .writeStream
      .format("memory")
      .outputMode(OutputMode.Complete())
      .queryName(storageOptions("memory.table.name"))
      .start
//      .trigger(Trigger.ProcessingTime("0 seconds"))
//            .partitionBy("environmentName", "deviceName", "metric", "timestamp")
//            .format("parquet")
//            .option("checkpointLocation", storageOptions("checkpoint.location"))
//            .option("path", storageOptions("parquet.path"))
//      .start()

    if (isTest) {
      rawKafkaMessages.writeStream.format("console").start()
      parsedStream.writeStream.format("console").start()
    }
  }
}