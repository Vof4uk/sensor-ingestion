package com.vmykytenko.sensors.query

import com.vmykytenko.sensors.collect.SensorMessage
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Encoders, SparkSession}

class SensorDashboardQuery() {
  /**
   *
   * @param kafkaConsumerOptions - map of options to serve input messages from Kafka. Must contain keys:
   *                             "subscribe" - A comma-separated list of topics;
   *                             "kafka.bootstrap.servers" - A comma-separated list of host:port
   *                             and may contain a bunch of optional, for details see
   *                             https://spark.apache.org/docs/3.3.0/structured-streaming-kafka-integration.html#creating-a-kafka-source-for-batch-queries
   */
  def apply(kafkaConsumerOptions: Map[String, String],
            kafkaProducerOptions: Map[String, String])(implicit spark: SparkSession): Unit = {

    implicit val valueEncoder = Encoders.product[SensorMessage]
    val topicOut = kafkaProducerOptions("topic")

    def dashboardReport(envName: String): SensorReport = {
      implicit val itemEncoder = Encoders.product[SensorReportItem]
      val items = spark
//        .read
//        .table("sensor_view")
        .sql("SELECT * FROM sensor_view;")
        .filter(col("environmentName").equalTo(envName))
        .as[SensorReportItem]
        .collect()

      SensorReport(items)
    }

    // TODO: it can be just String
    implicit val keyEncoder = Encoders.product[SensorReportRequestExt]
    val rawKafkaMessages = spark.readStream
      .format("kafka")
      .options(kafkaConsumerOptions)
      .load()
      .map(row => {
        val topicIn = row.getAs[String]("topic")
        val request =
          SensorReportRequestDeserializer.deserialize(topicIn, row.getAs[Array[Byte]]("value"))
        SensorReportRequestExt(
          request.environmentName,
          topicOut,
          SensorReportSerializer.serialize(topicOut, dashboardReport(request.environmentName))
        )
      })
      .writeStream
      .option("checkpointLocation", "./target/spark/checkpoints_feed/")
      .format("console")
      //            .outputMode("complete")
      .start()
    //      .select(
    //        col("respondTo").as("topic"),
    //        col("report").as("value")
    //      )
    //      .writeStream
    //      .format("kafka")
    //      .option("checkpointLocation", "./target/spark/checkpoints_query/")
    //      .options(kafkaProducerOptions)
    //      .start()

  }

}
