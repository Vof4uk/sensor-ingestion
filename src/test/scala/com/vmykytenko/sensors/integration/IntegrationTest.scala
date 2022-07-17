package com.vmykytenko.sensors.integration

import com.vmykytenko.sensors.collect._
import com.vmykytenko.sensors.query.SensorDashboardQuery
import com.vmykytenko.sensors._
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.sql.functions.{col, collect_list, lit, max_by}
import org.apache.spark.sql.{Encoders, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.utility.DockerImageName

import java.time.Duration
import java.util
import scala.collection.JavaConverters._

class IntegrationTest extends AnyFlatSpec with should.Matchers with BeforeAndAfterAll {
  private val KAFKA =
    new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.2.0"))
      .withEmbeddedZookeeper()
  KAFKA.start()

  private val SensorTopic = "sensor-topic-1"
  private val ReportTopic = "report-topic-1"
  private val ReportRespondTopic = "report-topic-2"

  def producerConfig(): util.Map[String, Object] =
    Map[String, Object](
      "bootstrap.servers" -> KAFKA.getBootstrapServers
    ).asJava

  def consumerConfig(): util.Map[String, Object] = Map[String, Object](
    "bootstrap.servers" -> KAFKA.getBootstrapServers,
    "auto.offset.reset" -> "earliest",
    "group.id" -> "console-consumer-myapp"
  ).asJava

  def getProducerReport() = new KafkaProducer[SensorReportRequestKey, SensorReportRequest](
    producerConfig(),
    SensorReportRequestKeySer,
    SensorReportRequestSer
  )

  def getProducerSensor() = new KafkaProducer[SensorMessageKey, SensorMessage](
    producerConfig(),
    SensorMessageKeySer,
    SensorMessageSer
  )

  def getConsumerSensor() = new KafkaConsumer[SensorMessageKey, SensorMessage](
    consumerConfig(),
    SensorMessageKeyDe,
    SensorMessageDe)

  def getConsumerReport() = new KafkaConsumer[SensorReportRequestKey, SensorReportRequest](
    consumerConfig(),
    SensorReportRequestKeyDe,
    SensorReportRequestDe)

  "Kafka container" should "serve messages" in {
    val producer = getProducerSensor()
    val consumer = getConsumerSensor()

    consumer.subscribe(List(SensorTopic).asJava)

    val key = SensorMessageKey("myEnv", "dev-id-3")
    val value = SensorMessage("myEnv", "dev-id-3", "metric-1", 0.01, 1L)
    producer.send(new ProducerRecord(SensorTopic, key, value)).get()
    producer.flush()

    val records = consumer.poll(Duration.ofSeconds(5))
    records.count() shouldBe 1
    records.iterator().next().key() shouldBe key
    records.iterator().next().value() shouldBe value
    consumer.close()
    producer.close()
  }

  "The system" should "query data" in {
    val producer1 = getProducerReport()
    val consumer1 = getConsumerReport()
    consumer1.subscribe(List(ReportRespondTopic).asJava)

    implicit val spark =
      SparkSession.builder()
        .appName(s"test-sensor-ingestion-${System.currentTimeMillis()}")
        .master("local[*]")
        .getOrCreate()

    SensorDashboardQuery(Map(
      "kafka.bootstrap.servers" -> KAFKA.getBootstrapServers,
      "subscribe" -> ReportTopic),
      Map("parquet.path" -> "./target/spark/tables/sensor_data_v2"),
      Map(
        "kafka.bootstrap.servers" -> KAFKA.getBootstrapServers,
        "topic" -> ReportRespondTopic
      )
    )

    val reportRequest = SensorReportRequest("myEnv2", "cid-01")
    val reportRequestKey = SensorReportRequestKey("myEnv2")
    val reportRequested = producer1.send(
      new ProducerRecord(
        ReportTopic,
        null,
        System.currentTimeMillis(),
        reportRequestKey,
        reportRequest)
    ).get()
    producer1.flush()

    val records = consumer1.poll(Duration.ofSeconds(10))
    val records2 = consumer1.poll(Duration.ofSeconds(10))

    println(records.count())
    println(records2.count())

    records.count() shouldBe 1
    records.iterator().next().key() shouldBe key
    records.iterator().next().value() shouldBe value

    consumer1.close()
    producer1.close()
    spark.stop()
    spark.close()
  }

  "The system" should "save data" in {
    val producer0 = getProducerSensor()

    implicit val spark =
      SparkSession.builder()
        .appName(s"test-sensor-ingestion-${System.currentTimeMillis()}")
        .master("local[*]")
        .getOrCreate()

    SensorDashboardFeed(KAFKA.getBootstrapServers, SensorTopic, "./target/spark/tables/sensor_data_v2")

    (1 to 20)
      .foreach(i => {
        val key = SensorMessageKey(s"myEnv${i % 7}", s"dev-id-2${i % 3}")
        val value = SensorMessage(s"myEnv${i % 7}", s"dev-id-2${i % 3}", "metric-1", 0.01 * i, System.currentTimeMillis())
        println(s"Sending message $value")
        val sentData = producer0.send(new ProducerRecord(SensorTopic, key, value)).get()
        producer0.flush()
        Thread.sleep(500)
      })


    producer0.close()
    spark.stop()
  }

  "Spark job" should "read parquet" in {
    val spark =
      SparkSession.builder()
        .appName(s"test-sensor-ingestion-${System.currentTimeMillis()}")
        .master("local[*]")
        .getOrCreate()

    implicit val itemEncoder = Encoders.product[SensorReportItem]

    val envName = "myEnv5"
    spark
      .read
      .parquet("./target/spark/tables/sensor_data_v2")
      .filter(col("environmentName").equalTo(envName))
      .groupBy(col("environmentName"), col("deviceName"))
      .agg(max_by(col("sensor_reading"), col("timestamp"))
        .as("sensor_reading")
      ).withColumn("environmentName", lit(envName))
      .groupBy(col("environmentName"))
      .agg(collect_list(col("sensor_reading")).as("sensor_reading"))
      .show(100)

  }

  override def afterAll() = {
    KAFKA.close()
  }

}
