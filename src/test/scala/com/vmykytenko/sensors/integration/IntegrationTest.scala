package com.vmykytenko.sensors.integration

import com.vmykytenko.sensors._
import com.vmykytenko.sensors.collect._
import com.vmykytenko.sensors.query.SensorDashboardQuery
import org.apache.commons.io.FileUtils
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.utility.DockerImageName

import java.nio.file.Paths
import java.time.Duration
import java.util
import java.util.UUID
import scala.collection.JavaConverters._

class IntegrationTest extends AnyFlatSpec with should.Matchers with BeforeAndAfterAll {
  private val KAFKA =
    new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.2.0"))
      .withEmbeddedZookeeper()
  KAFKA.start()

  case class TestConfig(id: String,
                        tempDirectory: String,
                        parquetDirectory: String,
                        sensorFeedTopic: String,
                        queryReportTopic: String,
                        listenReportTopic: String)

  case class SensorFeedApp(producer: KafkaProducer[SensorMessageKey, SensorMessage],
                           topic: String) {
    def close(): Unit = producer.close()
  }

  case class GetReportApp(producer: KafkaProducer[SensorReportRequestKey, SensorReportRequest],
                          topicProducer: String,
                          consumer: KafkaConsumer[SensorReportKey, SensorReport],
                          topicConsumer: String) {
    def close(): Unit = {
      consumer.close()
      producer.close()
    }
  }

  def getSensorFeedApplication(implicit sparkSession: SparkSession, testConfig: TestConfig): SensorFeedApp = {
    val storageOptions = Map(
      "checkpoint.location" -> s"${testConfig.tempDirectory}/feed",
      "parquet.path" -> s"${testConfig.parquetDirectory}",
      "memory.table.name" -> s"table_${testConfig.id.replace("-", "")}"
    )
    SensorDataFeed(
      KafkaConsumerConfig(
        servers = KAFKA.getBootstrapServers,
        topic = testConfig.sensorFeedTopic
      ),
      storageOptions,
      true)

    val producerConfig = Map[String, Object](
      "bootstrap.servers" -> KAFKA.getBootstrapServers
    ).asJava

    SensorFeedApp(
      topic = testConfig.sensorFeedTopic,
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
      "checkpoint.location" -> s"${testConfig.tempDirectory}/query",
      "parquet.path" -> s"${testConfig.parquetDirectory}",
      "memory.table.name" -> s"table_${testConfig.id.replace("-", "")}"
    )

    SensorDashboardQuery(
      KafkaConsumerConfig(
        servers = KAFKA.getBootstrapServers,
        topic = testConfig.queryReportTopic
      ),
      storageOptions,
      KafkaProducerConfig (
        servers = KAFKA.getBootstrapServers,
        topic = testConfig.listenReportTopic
      ), true)

    val consumer = new KafkaConsumer[SensorReportKey, SensorReport](
      consumerConfig,
      SensorReportKeyDe,
      SensorReportDe)

    consumer.subscribe(List(testConfig.listenReportTopic).asJava)
    GetReportApp(
      producer = new KafkaProducer[SensorReportRequestKey, SensorReportRequest](
        producerConfig,
        SensorReportRequestKeySer,
        SensorReportRequestSer
      ),
      topicProducer = testConfig.queryReportTopic,
      consumer = consumer,
      topicConsumer = testConfig.listenReportTopic
    )
  }

  implicit val spark = SparkSession.builder()
    .appName(s"test-sensor-${System.currentTimeMillis()}")
    .master("local[8]")
    .getOrCreate()

  def env1device1metric1 =
    SensorMessageKey("myEnv-1", s"dev-id-1") ->
      SensorMessage(s"myEnv-1", s"dev-id-1", "metric-1", 0.01, System.currentTimeMillis())

  def env1device1metric2 =
    SensorMessageKey("myEnv-1", s"dev-id-1") ->
      SensorMessage(s"myEnv-1", s"dev-id-1", "metric-2", 0.09, System.currentTimeMillis())

  def env2device1metric1 =
    SensorMessageKey("myEnv-2", s"dev-id-1") ->
      SensorMessage(s"myEnv-2", s"dev-id-1", "metric-1", 0.12, System.currentTimeMillis())


  def sendTo[K, V](topic: String, producer: KafkaProducer[K, V], message: (K, V)) = {
    producer.send(new ProducerRecord(topic, message._1, message._2))
    producer.flush()
  }

  def getResponse[K, V](topicConsumer: String, consumer: KafkaConsumer[K, V]): List[(K, V)] = {
    val poll = consumer.poll(Duration.ofSeconds(20))
    val list = new util.ArrayList[(K, V)]()
    poll.forEach(r => list.add(r.key() -> r.value()))
    list.asScala.toList
  }

  def getTestConfig(): TestConfig = {
    val id = UUID.randomUUID().toString
    val testDirectory = s"./target/spark/test/${this.getClass.getSimpleName}/$id}"
    val parquetDirectory = s"$testDirectory/tables"
    FileUtils.createParentDirectories(Paths.get(parquetDirectory + "/dummy").toFile)
    TestConfig(
      id,
      tempDirectory = testDirectory,
      parquetDirectory = parquetDirectory,
      sensorFeedTopic = s"feed-${System.currentTimeMillis()}",
      queryReportTopic = s"query-report-${System.currentTimeMillis()}",
      listenReportTopic = s"listen-report-${System.currentTimeMillis()}"
    )
  }

  "The system" should "answer empty report, when no records" in {
    implicit val testConfig = getTestConfig()

    val feedApp = getSensorFeedApplication

    val reportApp = getSensorViewQueryApplication

    Thread.sleep(20000)
    sendTo(reportApp.topicProducer, reportApp.producer,
      (SensorReportRequestKey(env1device1metric1._1.environmentName),
        SensorReportRequest(env1device1metric1._1.environmentName, "cid-1")))
    val response = getResponse(reportApp.topicConsumer, reportApp.consumer)

    feedApp.close()
    reportApp.close()

    response.length shouldBe 1
    response.head._1 shouldBe SensorReportKey(env1device1metric1._1.environmentName)
    response.head._2.cid shouldBe "cid-1"
    response.head._2.items should contain theSameElementsAs (List())
  }

  "The system" should "save and return latest reports, exactly same environmentName" in {
    implicit val testConfig = getTestConfig()
    val sensorFeedApp = getSensorFeedApplication
    val reportApp = getSensorViewQueryApplication


    (1 to 20).foreach { _ =>
      sendTo(sensorFeedApp.topic, sensorFeedApp.producer, env1device1metric1)
      Thread.sleep(250)
      sendTo(sensorFeedApp.topic, sensorFeedApp.producer, env1device1metric2)
      Thread.sleep(250)
      sendTo(sensorFeedApp.topic, sensorFeedApp.producer, env2device1metric1)
      Thread.sleep(250)
    }
    val expectedSensor1device1metric1 = env1device1metric1

    Thread.sleep(20000)

    sendTo(reportApp.topicProducer, reportApp.producer,
      (SensorReportRequestKey(env1device1metric1._1.environmentName),
        SensorReportRequest(env1device1metric1._1.environmentName, "cid-1")))
    var response = getResponse(reportApp.topicConsumer, reportApp.consumer)
    if (response.isEmpty) {
      response = getResponse(reportApp.topicConsumer, reportApp.consumer)
    }

    reportApp.close()
    sensorFeedApp.close()

    response.length shouldBe 1
    response.head._1 shouldBe SensorReportKey(env1device1metric1._1.environmentName)
    response.head._2.cid shouldBe "cid-1"
    val items = response.head._2.items
    items.length shouldBe 2
    items.forall(_.environmentName == "myEnv-1") shouldBe true
    items.forall(_.deviceName == "dev-id-1") shouldBe true
    items.exists(_.metric == "metric-1") shouldBe true
    items.exists(_.metric == "metric-2") shouldBe true
    items.forall(item => Math.abs(expectedSensor1device1metric1._2.timestamp - item.timestamp) < Duration.ofSeconds(5).toMillis)
  }

  override def afterAll() = {
    KAFKA.close()
    spark.stop()
    spark.close()
  }
}
