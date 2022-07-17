package com.vmykytenko.sensors.query

import com.vmykytenko.sensors.{SensorMessage, SensorReportRequest, SensorReportRequestDe}
import org.apache.commons.codec.StringEncoder
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.functions.{col, current_timestamp, expr, from_unixtime, window}
import org.apache.spark.sql.{Dataset, Encoder, Encoders, Row, SparkSession}

import java.sql.Timestamp

/**
 * Reduces the SensorMessage by device id, leaving tha latest.
 */
case class ReportRawData(cid: String,
                         requestedAt: Timestamp,
                         environmentName: String,
                         deviceName: String,
                         metric: String,
                         timestamp: Long,
                         value: Double)

case class SensorReportRequestIn(requestedAt: Timestamp, cid: String, environmentName: String)

case class CompiledReportMessage(key: Array[Byte], value: Array[Byte])

case object SensorDashboardQuery {

  private def parsedRequests(kafkaConsumerOptions: Map[String, String])
                            (implicit spark: SparkSession, requestEncoder: Encoder[SensorReportRequestIn]): Dataset[SensorReportRequestIn] = {

    // TODO: it can be just String
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
            kafkaProducerOptions: Map[String, String])(implicit spark: SparkSession): Unit = {

    implicit val sensorMsgEncoder = Encoders.product[SensorMessage]
    implicit val requestEncoder = Encoders.product[SensorReportRequestIn]
    implicit val reportRawEncoder = Encoders.product[ReportRawData]
    implicit val stringEncoder = ExpressionEncoder[String]()


    val sensorsView: Dataset[SensorMessage] = spark
      .read
      .parquet(storageOptions("parquet.path"))
      .as[SensorMessage]

    // TODO: it can be just String
    val parsedReq = parsedRequests(kafkaConsumerOptions)

    val reportRawJoin = parsedReq // Fix no response if join is empty
      .join(sensorsView, "environmentName")
      .as[ReportRawData]

    reportRawJoin.printSchema()

    val reportAggregator = new ReportAggregator().toColumn
    val responses =
      reportRawJoin
        .select(reportAggregator)
    //        .withWatermark("requestedAt", "1 second")
    //        .groupByKey(_.environmentName)
    //          col("environmentName"),
    //          window(col("requestedAt"), "1 second"))
    //        .agg(
    //          reportAggregator
    //        )
    //        .select(
    //          col("reportMessage.key").as("key"),
    //          col("reportMessage.value").as("value")
    //        )


    //    responses
    //      .writeStream
    //      .option("checkpointLocation", "./target/spark/checkpoints_query/22")
    //      .format("console")
    //                  .outputMode("complete")
    //      .start()

    //    rawKafkaMessages
    //      .writeStream
    //      .option("checkpointLocation", "./target/spark/checkpoints_feed/33")
    //      .format("console")
    //      //            .outputMode("complete")
    //      .start()
    //      .select(
    //        col("respondTo").as("topic"),
    //        col("report").as("value")
    //      )
    responses
      .writeStream
      .format("kafka")
      .outputMode("complete")
      .option("checkpointLocation", "./target/spark/checkpoints_query/")
      .options(kafkaProducerOptions)
      .start()

  }

}
