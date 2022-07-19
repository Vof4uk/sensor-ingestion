package com.vmykytenko.sensors.query

import com.vmykytenko.sensors.{SensorReport, SensorReportItem, SensorReportKey}
import org.apache.kafka.common.serialization.Serializer
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.expressions.Aggregator

import scala.collection.mutable
case class AggKey(environmentName: String, deviceName: String, metric: String)

/**
 * Used for Spark Aggregation of groups.
 * Finds records with latest Timestamp for each unique combination of environmentName, deviceName and
 * metric.
 * Returns Report Value and Key ready for Kafka serialization.
 */
class ReportAggregator() extends Aggregator[ReportRawData, mutable.Map[AggKey, ReportRawData], CompiledReport] {
  override def zero: mutable.Map[AggKey, ReportRawData] = mutable.Map.empty

  private def isNewer(oldValue: ReportRawData, newValue: ReportRawData) = {
    oldValue.timestamp < newValue.timestamp
  }

  override def reduce(acc: mutable.Map[AggKey, ReportRawData], msg: ReportRawData): mutable.Map[AggKey, ReportRawData] = {
    val mustAdd = msg.deviceName != ReportRawData.empty.deviceName &&
      acc.get(AggKey(msg.environmentName, msg.deviceName, msg.metric)).forall { old =>
        isNewer(old, msg)
      }
    if (mustAdd || acc.isEmpty) { // already checked msg.deviceName
      acc.put(AggKey(msg.environmentName, msg.deviceName, msg.metric), msg)
    }
    acc
  }

  override def merge(b1: mutable.Map[AggKey, ReportRawData], b2: mutable.Map[AggKey, ReportRawData]): mutable.Map[AggKey, ReportRawData] = {
    val merged = (b1.keySet ++ b2.keySet).map(k =>
      (b1.get(k), b2.get(k)) match {
        case (Some(v), None) => k -> v
        case (None, Some(v)) => k -> v
        case (Some(old), Some(newer)) if isNewer(old, newer) => k -> newer // TODO: Non deterministic
        case (Some(v1), Some(_)) => k -> v1
      }).toSeq

    mutable.Map(merged: _*)
  }

  override def finish(reduction: mutable.Map[AggKey, ReportRawData]): CompiledReport = {
    if (reduction.isEmpty) {
      CompiledReport(
        key = SensorReportKey.empty,
        value = SensorReport.empty
      )
    } else {
      val environmentName = reduction.head._2.environmentName
      val cid = reduction.head._2.cid
      CompiledReport( // todo inject topic here
        key = SensorReportKey(environmentName),
        value = SensorReport(
          cid = cid,
          items = reduction.collect {
            case (key, data) if key.deviceName != ReportRawData.empty.deviceName =>
              SensorReportItem(
                data.environmentName,
                data.deviceName,
                data.metric,
                data.value,
                data.timestamp)
          }.toArray
        )
      )
    }
  }

  override def bufferEncoder: Encoder[mutable.Map[AggKey, ReportRawData]] =
    ExpressionEncoder[mutable.Map[AggKey, ReportRawData]]

  override def outputEncoder: Encoder[CompiledReport] =
    ExpressionEncoder[CompiledReport]
}
