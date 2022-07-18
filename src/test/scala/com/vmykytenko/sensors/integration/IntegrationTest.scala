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
import java.util.UUID
import scala.collection.JavaConverters._

class IntegrationTest extends AnyFlatSpec with should.Matchers with BeforeAndAfterAll {
  private val KAFKA =
    new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.2.0"))
      .withEmbeddedZookeeper()
  KAFKA.start()

  val MillisNow = 1658087210173L

  private val SensorTopic = "sensor-topic-1"
  private val ReportTopic = "report-topic-1"
  private val ReportRespondTopic = "report-topic-2"

  case class TestConfig(testDirectory: String)

  case class SensorFeedApp(producer: KafkaProducer[SensorMessageKey, SensorMessage],
                           topic: String)

  case class GetReportApp(producer: KafkaProducer[SensorReportRequestKey, SensorReportRequest],
                          topicProducer: String,
                          consumer: KafkaConsumer[SensorReportKey, SensorReport],
                          topicConsumer: String)

  def getSensorFeedApplication(implicit sparkSession: SparkSession, testConfig: TestConfig): SensorFeedApp = {
    val storageOptions = Map(
      "checkpoint.location" -> s"${testConfig.testDirectory}/feed",
      "parquet.path" -> s"${testConfig.testDirectory}/tables"
    )
    SensorDashboardFeed(
      KAFKA.getBootstrapServers,
      SensorTopic,
      storageOptions,
      true)

    val producerConfig = Map[String, Object](
      "bootstrap.servers" -> KAFKA.getBootstrapServers
    ).asJava

    SensorFeedApp(
      topic = SensorTopic,
      producer = new KafkaProducer[SensorMessageKey, SensorMessage](
        producerConfig,
        SensorMessageKeySer,
        SensorMessageSer)
    )
  }

  def getSensorViewQueryApplication(implicit sparkSession: SparkSession, testConfig: TestConfig): GetReportApp = {

    val consumerConfig = Map[String, Object](
      "bootstrap.servers" -> KAFKA.getBootstrapServers,
      "auto.offset.reset" -> "earliest",
      "group.id" -> "console-consumer-myapp"
    ).asJava

    val producerConfig = Map[String, Object](
      "bootstrap.servers" -> KAFKA.getBootstrapServers
    ).asJava

    val storageOptions = Map(
      "checkpoint.location" -> s"${testConfig.testDirectory}/query",
      "parquet.path" -> s"${testConfig.testDirectory}/tables"
    )

    SensorDashboardQuery(
      Map(
      "kafka.bootstrap.servers" -> KAFKA.getBootstrapServers,
      "subscribe" -> ReportTopic),
      storageOptions,
      Map(
        "kafka.bootstrap.servers" -> KAFKA.getBootstrapServers,
        "topic" -> ReportRespondTopic
      ), true)

    val consumer = new KafkaConsumer[SensorReportKey, SensorReport](
      consumerConfig,
      SensorReportKeyDe,
      SensorReportDe)

    consumer.subscribe(List(ReportRespondTopic).asJava)
    GetReportApp(
      producer = new KafkaProducer[SensorReportRequestKey, SensorReportRequest](
        producerConfig,
        SensorReportRequestKeySer,
        SensorReportRequestSer
      ),
      topicProducer = ReportTopic,
      consumer = consumer,
      topicConsumer = ReportRespondTopic
    )
  }

  def getTestSparkSession() = SparkSession.builder()
    .appName(s"test-sensor-${System.currentTimeMillis()}")
    .master("local[8]")
    .getOrCreate()

  val sensor1device1oldest =
    SensorMessageKey("myEnv-1", s"dev-id-1") ->
      SensorMessage(s"myEnv1", s"dev-id-1", "metric-1", 0.01, MillisNow)

  val sensor1device1newest =
    SensorMessageKey("myEnv-1", s"dev-id-1") ->
      SensorMessage(s"myEnv1", s"dev-id-1", "metric-1", 0.09, MillisNow + 100)


  def sendTo[K, V](topic: String, producer: KafkaProducer[K, V], message: (K, V)) = {
    producer.send(new ProducerRecord(topic, message._1, message._2))
    producer.flush()
  }

  def getResponse[K, V](topicConsumer: String, consumer: KafkaConsumer[K, V]): List[(K, V)] = {
    val poll = consumer.poll(Duration.ofSeconds(5))
    val list = new util.ArrayList[(K, V)]()
    poll.forEach(r => list.add(r.key() -> r.value()))
    list.asScala.toList
  }

  def getTestConfig(): TestConfig = {
    TestConfig(
      testDirectory = s"./target/spark/test/${this.getClass.getSimpleName}/${UUID.randomUUID().toString}"
    )
  }

  "The system" should "save and return latest data" in {
    implicit val testConfig = getTestConfig()
    implicit val spark = getTestSparkSession()
    val sensorFeedApp = getSensorFeedApplication

    sendTo(sensorFeedApp.topic, sensorFeedApp.producer, sensor1device1oldest)
    sendTo(sensorFeedApp.topic, sensorFeedApp.producer, sensor1device1newest)

    Thread.sleep(60000)

    val reportApp = getSensorViewQueryApplication
    sendTo(reportApp.topicProducer, reportApp.producer,
      (SensorReportRequestKey(sensor1device1oldest._1.environmentName),
        SensorReportRequest(sensor1device1oldest._1.environmentName, "cid-1")))
    val response = getResponse(reportApp.topicConsumer, reportApp.consumer)

    response.length shouldBe 1
    response.head._1 shouldBe SensorReportKey(sensor1device1oldest._1.environmentName)
    response.head._2.cid shouldBe "cid-1"
    response.head._2.items should contain theSameElementsAs (List())

  }

  "Spark job" should "read parquet" in {
    val spark =
      getTestSparkSession()

    implicit val itemEncoder = Encoders.product[SensorReportItem]

    val envName = "myEnv5"
    spark
      .read
      .parquet("ParquetPath")
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
