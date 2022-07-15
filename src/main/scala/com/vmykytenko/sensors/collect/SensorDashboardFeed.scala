package com.vmykytenko.sensors.collect

import org.apache.spark.sql.functions.{col, count, first, from_unixtime, max, max_by, timestamp_seconds, to_timestamp, udf, window}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{BinaryType, LongType, StringType}
import org.apache.spark.sql.{Encoders, SparkSession}
import org.apache.spark.streaming.Seconds

import scala.concurrent.duration.{Duration, DurationInt}

class SensorDashboardFeed(val backupInterval: Duration) {

  /**
   *
   * @param kafkaConsumerOptions - map of options to serve input messages from Kafka. Must contain keys:
   *                             "subscribe" - A comma-separated list of topics;
   *                             "kafka.bootstrap.servers" - A comma-separated list of host:port
   *                             and may contain a bunch of optional, for details see
   *                             https://spark.apache.org/docs/3.3.0/structured-streaming-kafka-integration.html#creating-a-kafka-source-for-batch-queries
   */
  def apply(servers: String,
            topic: String)(implicit spark: SparkSession): Unit = {
    val parseSensorData = udf((bytes: Array[Byte], topic: String) => SensorMessageDeserializer.deserialize(topic, bytes))
    spark.udf.register("parseSensorData", parseSensorData)

    val rawKafkaMessages = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", servers)
      .option("subscribe", topic)
      .option("startingOffsets", "latest")
      .load()



    implicit val encoderStored = Encoders.product[StoreSensorData]
    implicit val encoderIn = Encoders.product[SensorMessage]
    val mapped = rawKafkaMessages
      .select(
        parseSensorData(col("value"), col("topic")).as("event").cast(encoderIn.schema),
        col("timestamp").as("eventTime")
      )
      .withWatermark("eventTime", "5 seconds")
      .select(
        col("event"),
        col("eventTime"),
        col("event.environmentName").as("environmentName").cast(StringType),
        col("event.deviceName").as("deviceName").cast(StringType),
        window(col("eventTime"), "5 seconds").as("window")
      )

    val grouped = mapped
      .groupBy("environmentName", "deviceName", "window")
      .agg(
        max_by(col("eventTime"), col("eventTime").cast(LongType))
          .as("eventTime"),
        max_by(col("event"), col("eventTime").cast(LongType))
          .as("event")
      )

//    grouped
//      .createOrReplaceTempView("sensor_view")

    grouped
      .drop(col("window"))
      .writeStream
      .format("parquet")
      .option("checkpointLocation", "./checkpoints_feed/parquet")
      .option("path", "./tables/sensor_view")
      .start()

//    grouped
//      .writeStream
//      .queryName("grouped")
////      .trigger(Trigger.ProcessingTime(1.seconds))
//      .option("checkpointLocation", "./target/spark/checkpoints_feed/3")
//      .format("console")
//      .outputMode("complete")
//      .start()

//    spark.sql("SELECT * FROM sensor_view;")
//      .writeStream
//      .trigger(Trigger.ProcessingTime(1.minute))
//      .option("checkpointLocation", "./target/spark/checkpoints_feed/4")
//      .format("console")
//      .start()


    //          .createOrReplaceGlobalTempView("")

    //          .writeStream
    //          .option("checkpointLocation", "./checkpoints_feed/")
    //          .format("parquet")
    //          .option("path", "./tables/sensor_view")
    //          .start

    //      .outputMode("complete")
    //      .toTable("sensor_view")


  }
}

case class StoreSensorData(environmentName: String,
                           deviceName: String,
                           metric: String,
                           value: Double,
                           timestamp: Long,
                           kafkaTimestamp: Long)