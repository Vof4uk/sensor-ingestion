package com.vmykytenko.sensors.query

import com.vmykytenko.sensors._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.functions.{coalesce, col, lit}
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession}

import java.sql.Timestamp
import java.time.LocalDateTime

/** Data after join and required for report. */
case class ReportRawData(cid: String,
                         requestedAt: Timestamp,
                         environmentName: String,
                         deviceName: String,
                         metric: String,
                         timestamp: Long,
                         value: Double)

object ReportRawData {
  val empty = ReportRawData(
    cid = "",
    requestedAt = Timestamp.valueOf(LocalDateTime.MIN),
    environmentName = "",
    deviceName = "",
    metric = "",
    timestamp = Long.MinValue,
    value = Double.MinValue
  )
}

/** Request data from user, sufficient to compile a report. */
case class SensorReportRequestIn(requestedAt: Timestamp, cid: String, environmentName: String)

/** Report Message data read for serialization. */
case class CompiledReport(key: SensorReportKey, value: SensorReport)

/** Message ready for Kafka stream out. */
case class CompiledReportMessage(key: Array[Byte], value: Array[Byte])


/**
 * Reduces the SensorMessage by device id, leaving tha latest.
 * Uses Parquet filled by other apps as a Datasource.
 */
case object SensorDashboardQuery {

  private def parsedRequests(kafkaConsumerOptions: Map[String, String])
                            (implicit spark: SparkSession, requestEncoder: Encoder[SensorReportRequestIn]): Dataset[SensorReportRequestIn] = {

    val rawKafkaMessages = spark.readStream
      .format("kafka")
      .options(kafkaConsumerOptions)
      .option("startingOffsets", "earliest") // TODO:
      .load()

    rawKafkaMessages
      .map(row => {
        val topic = row.getAs[String]("topic")
        val value = row.getAs[Array[Byte]]("value")
        val clientRequest = SensorReportRequestDe.deserialize(topic, value)
        SensorReportRequestIn(
          requestedAt = row.getAs("timestamp"),
          cid = clientRequest.cid,
          environmentName = clientRequest.environmentName
        )
      })
  }

  /**
   *
   * @param kafkaConsumerOptions - map of options to serve input messages from Kafka. Must contain keys:
   *                             "subscribe" - A comma-separated list of topics;
   *                             "kafka.bootstrap.servers" - A comma-separated list of host:port
   *                             and may contain a bunch of optional, for details see
   *                             https://spark.apache.org/docs/3.3.0/structured-streaming-kafka-integration.html#creating-a-kafka-source-for-batch-queries
   */
  def apply(kafkaConsumerOptions: Map[String, String],
            storageOptions: Map[String, String],
            kafkaProducerOptions: Map[String, String],
            isTest: Boolean)(implicit spark: SparkSession): Unit = {

    implicit val sensorMsgEncoder = Encoders.product[SensorMessage]
    implicit val requestEncoder = Encoders.product[SensorReportRequestIn]
    implicit val reportRawEncoder = Encoders.product[ReportRawData]
    implicit val reportMessageEncoder = Encoders.product[CompiledReportMessage]
    implicit val stringEncoder = ExpressionEncoder[String]()


    val sensorsView: Dataset[SensorMessage] = spark
      .read
      .schema(implicitly[Encoder[SensorMessage]].schema)
      .parquet(storageOptions("parquet.path"))
      .as[SensorMessage]

    val parsedReq = parsedRequests(kafkaConsumerOptions)

    val reportRawJoin = parsedReq
      .join(
        sensorsView,
        List("environmentName", "environmentName"),
        "left_outer")
      // Client calls that have no data in source db after JOIN will produce NULLs
      .select(
        col("environmentName"),
        col("requestedAt"),
        col("cid"),
        coalesce(col("deviceName"), lit(ReportRawData.empty.deviceName)).as("deviceName"),
        coalesce(col("metric"), lit(ReportRawData.empty.metric)).as("metric"),
        coalesce(col("value"), lit(ReportRawData.empty.value)).as("value"),
        coalesce(col("timestamp"), lit(ReportRawData.empty.timestamp)).as("timestamp"))
      .as[ReportRawData]

    val reportAggregator =
      new ReportAggregator().toColumn
    val compiledReports = reportRawJoin
      .select(reportAggregator)
      // Empty micro-batches produce noise when aggregated,
      // this is a trade-off to send responses for env that have no results.
      .filter(_.key != SensorReportKey.empty)

    compiledReports
      .map(report =>
        CompiledReportMessage(
          key = SensorReportKeySer.serialize("", report.key),
          value = SensorReportSer.serialize("", report.value)
        ))
      .writeStream
      .format("kafka")
      .outputMode("complete")
      .option("checkpointLocation", storageOptions("checkpoint.location"))
      .options(kafkaProducerOptions)
      .start()

    if (isTest) {
      parsedReq.writeStream.format("console").start()
      reportRawJoin.writeStream.option("numRows", 200).format("console").start()
      compiledReports.writeStream.outputMode("complete").format("console").start()
    }
  }

}
