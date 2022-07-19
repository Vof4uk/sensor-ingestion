package com.vmykytenko.sensors.query

import com.vmykytenko.sensors.SensorReportItem
import org.apache.spark.sql.{Encoders, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

import java.sql.Timestamp

class ReportAggregatorTest extends AnyFlatSpec with should.Matchers with BeforeAndAfterAll {
  val spark = SparkSession.builder()
    .appName(s"test-sensor-${System.currentTimeMillis()}")
    .master("local[*]")
    .getOrCreate()

  val Cid = "cid-1"
  val Env_1 = "env-1"
  val Env_2 = "env-2"
  val Dev_1 = "dev-1"
  val Dev_2 = "dev-2"
  val Met_1 = "met-1"
  val Met_2 = "met-2"

  behavior of "ReportAggregator"
  it should "select newest timestamp from group" in {
    val aggregator = new ReportAggregator().toColumn
    val timestamp = now();
    val millis = System.currentTimeMillis();
    val group = Seq(
      ReportRawData(Cid, timestamp, Env_1, Dev_1, Met_1, millis, 0.01),
      ReportRawData(Cid, timestamp, Env_1, Dev_1, Met_1, millis + 1, 0.02)
    )

    implicit val enc = Encoders.product[ReportRawData]
    val reports = spark.createDataFrame(group)
      .as[ReportRawData]
      .select(aggregator)
      .collectAsList()


    reports.size() shouldBe 1
    val report = reports.get(0)
    report.value.items.length shouldBe 1
    val expectedItem = SensorReportItem(Env_1, Dev_1, Met_1, 0.02, millis + 1)
    report.value.items should contain theSameElementsAs List(expectedItem)
  }

  it should "separate groups when different envName" in {
    val aggregator = new ReportAggregator().toColumn
    val timestamp = now();
    val millis = System.currentTimeMillis();
    val group = Seq(
      ReportRawData(Cid, timestamp, Env_1, Dev_1, Met_1, millis, 0.01),
      ReportRawData(Cid, timestamp, Env_2, Dev_1, Met_1, millis + 1, 0.02)
    )

    implicit val enc = Encoders.product[ReportRawData]
    val reports = spark.createDataFrame(group)
      .as[ReportRawData]
      .select(aggregator)
      .collectAsList()

    reports.size() shouldBe 1
    reports.get(0).value.items.length shouldBe 2
  }

  it should "separate groups when different deviceName" in {
    val aggregator = new ReportAggregator().toColumn
    val timestamp = now();
    val millis = System.currentTimeMillis();
    val group = Seq(
      ReportRawData(Cid, timestamp, Env_1, Dev_1, Met_1, millis, 0.01),
      ReportRawData(Cid, timestamp, Env_1, Dev_2, Met_1, millis + 1, 0.02)
    )

    implicit val enc = Encoders.product[ReportRawData]
    val reports = spark.createDataFrame(group)
      .as[ReportRawData]
      .select(aggregator)
      .collectAsList()

    reports.size() shouldBe 1
    reports.get(0).value.items.length shouldBe 2
  }

  it should "separate groups when different metric name" in {
    val aggregator = new ReportAggregator().toColumn
    val timestamp = now();
    val millis = System.currentTimeMillis();
    val group = Seq(
      ReportRawData(Cid, timestamp, Env_1, Dev_1, Met_1, millis, 0.01),
      ReportRawData(Cid, timestamp, Env_1, Dev_1, Met_2, millis + 1, 0.02)
    )

    implicit val enc = Encoders.product[ReportRawData]
    val reports = spark.createDataFrame(group)
      .as[ReportRawData]
      .select(aggregator)
      .collectAsList()

    reports.size() shouldBe 1
    reports.get(0).value.items.length shouldBe 2
  }


  private def now(): Timestamp = {
    new Timestamp(System.currentTimeMillis())
  }
  override def afterAll(): Unit = {
    spark.stop()
    spark.close()
  }

}
