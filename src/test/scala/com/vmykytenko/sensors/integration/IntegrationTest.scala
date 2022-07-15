package com.vmykytenko.sensors.integration

import com.vmykytenko.sensors.collect._
import com.vmykytenko.sensors.query.{SensorDashboardQuery, SensorReport, SensorReportDeserializer, SensorReportRequest, SensorReportRequestSerializer}
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.header.Header
import org.apache.kafka.common.header.internals.RecordHeader
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should
import org.scalatest.time.SpanSugar.convertIntToGrainOfTime
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.utility.DockerImageName

import java.time.Duration
import java.util
import java.util.UUID
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

  def getProducerReport() = new KafkaProducer[SensorReportRequest, SensorReportRequest](
    producerConfig(),
    SensorReportRequestSerializer,
    SensorReportRequestSerializer
  )

  def getProducerSensor() = new KafkaProducer[SensorMessageKey, SensorMessage](
    producerConfig(),
    SensorMessageKeySerializer,
    SensorMessageSerializer
  )

  def getConsumerSensor() = new KafkaConsumer[SensorMessageKey, SensorMessage](
    consumerConfig(),
    SensorMessageKeyDeserializer,
    SensorMessageDeserializer)

  def getConsumerReport() = new KafkaConsumer[String, SensorReport](
    consumerConfig(),
    new StringDeserializer(),
    SensorReportDeserializer)

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

  "The system" should "save data" in {
    val producer0 = getProducerSensor()
    val producer1 = getProducerReport()
    val consumer1 = getConsumerReport()

    consumer1.subscribe(List(ReportRespondTopic).asJava)

    implicit val spark =
      SparkSession.builder()
        .appName(s"test-sensor-ingestion-${System.currentTimeMillis()}")
        .master("local[*]")
        .getOrCreate()

    new SensorDashboardFeed(1.minute)
      .apply(KAFKA.getBootstrapServers, SensorTopic)

//    new SensorDashboardQuery().apply(Map(
//      "kafka.bootstrap.servers" -> KAFKA.getBootstrapServers,
//      "subscribe" -> ReportTopic),
//      Map(
//        "kafka.bootstrap.servers" -> KAFKA.getBootstrapServers,
//        "topic" -> ReportRespondTopic
//      )
//    )



    val reportKey = SensorReportRequest("myEnv")

    (1 to 100)
      .foreach( i => {
        val key = SensorMessageKey(s"myEnv${i % 7}", s"dev-id-2${i % 3}")
        val value = SensorMessage(s"myEnv${i % 7}", s"dev-id-2${i % 3}", "metric-1", 0.01 * i, System.currentTimeMillis())
        println(s"Sending message $value")
        val sentData = producer0.send(new ProducerRecord(SensorTopic, key, value)).get()
        producer0.flush()
        Thread.sleep(1000)
      })

    val reportRequested = producer1.send(
      new ProducerRecord(
        ReportTopic,
        null,
        System.currentTimeMillis(),
        reportKey,
        reportKey)
    ).get()
    producer1.flush()

    val records = consumer1.poll(Duration.ofSeconds(5))

    consumer1.close()
    producer0.close()
    producer1.close()
    spark.stop()

    records.count() shouldBe 1
    records.iterator().next().key() shouldBe key
    records.iterator().next().value() shouldBe value

  }

  "Spark job" should "read parquet" in {
    val spark =
      SparkSession.builder()
        .appName(s"test-sensor-ingestion-${System.currentTimeMillis()}")
        .master("local[*]")
        .getOrCreate()

    spark.read
      .parquet("./tables/sensor_view")
      .show(1000)


  }

  override def afterAll() = {
    KAFKA.close()
  }

}
