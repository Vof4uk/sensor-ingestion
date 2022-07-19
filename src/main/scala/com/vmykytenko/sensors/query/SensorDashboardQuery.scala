package com.vmykytenko.sensors.query

import com.vmykytenko.sensors._
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
 * Listens to client requests.
 * On each request for sensor report, queries datastore for events with "environmentName", and
 * aggregates rows into a single row with report. Sends the report to a pre-configured Kafka topic.
 */
case object SensorDashboardQuery {

  private def parsedRequests(consumerConfig: KafkaConsumerConfig)
                            (implicit spark: SparkSession, requestEncoder: Encoder[SensorReportRequestIn]): Dataset[SensorReportRequestIn] = {

    val rawKafkaMessages = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", consumerConfig.servers)
      .option("subscribe", consumerConfig.topic)
      .option("startingOffsets", "earliest")
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

  def apply(consumerConfig: KafkaConsumerConfig,
            applicationStorageConfig: ApplicationStorageConfig,
            producerConfig: KafkaProducerConfig,
            isTest: Boolean)(implicit spark: SparkSession): Unit = {

    implicit val sensorMsgEncoder = Encoders.product[SensorMessage]
    implicit val requestEncoder = Encoders.product[SensorReportRequestIn]
    implicit val reportRawEncoder = Encoders.product[ReportRawData]
    implicit val reportMessageEncoder = Encoders.product[CompiledReportMessage]


    val sensorsView: Dataset[SensorMessage] = spark
//      .read
//      .schema(implicitly[Encoder[SensorMessage]].schema)
//      .parquet(applicationStorageConfig.parquetPath)
//      .as[SensorMessage]
      .sql(s"select * from ${applicationStorageConfig.memorySinkName}")
      .as[SensorMessage]


    val parsedReq = parsedRequests(consumerConfig)

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
      .option("checkpointLocation", applicationStorageConfig.checkpointLocation)
      .option("topic", producerConfig.topic)
      .option("kafka.bootstrap.servers", producerConfig.servers)
      .start()

    if (isTest) {
      parsedReq.writeStream.format("console").start()
      reportRawJoin.writeStream.option("numRows", 200).format("console").start()
      compiledReports.writeStream.outputMode("complete").format("console").start()
    }
  }

}
