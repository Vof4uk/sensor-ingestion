package com.vmykytenko.sensors.collect

import com.vmykytenko.sensors.{ApplicationStorageConfig, KafkaConsumerConfig, SensorMessage, SensorMessageDe}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{Encoders, SparkSession}

/**
 * Consumes sensor data and saves to shared datastore.
 */
case object SensorDataFeed {

  def apply(consumerConfig: KafkaConsumerConfig,
            applicationStorageConfig: ApplicationStorageConfig,
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
      .queryName(applicationStorageConfig.memorySinkName)
//            .partitionBy("environmentName", "deviceName", "metric", "timestamp")
//            .format("parquet")
//            .option("checkpointLocation", applicationStorageConfig.checkpointLocation)
//            .option("path", applicationStorageConfig.parquetPath)
      .start()

    if (isTest) {
      rawKafkaMessages.writeStream.format("console").start()
      parsedStream.writeStream.format("console").start()
    }
  }
}