package com.vmykytenko.sensors.query

import com.vmykytenko.sensors.{SensorReport, SensorReportItem, SensorReportKey}
import org.apache.kafka.common.serialization.Serializer
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.expressions.Aggregator

import scala.collection.mutable

class ReportAggregator(valueSerializer: Serializer[SensorReport],
                       keySerializer: Serializer[SensorReportKey]) extends Aggregator[ReportRawData, mutable.Map[String, ReportRawData], CompiledReport] {
  override def zero: mutable.Map[String, ReportRawData] = mutable.Map.empty

  private def isNewer(oldValue: ReportRawData, newValue: ReportRawData) = {
    oldValue.timestamp < newValue.timestamp
  }

  override def reduce(acc: mutable.Map[String, ReportRawData], msg: ReportRawData): mutable.Map[String, ReportRawData] = {
    val mustAdd = msg.deviceName != ReportRawData.empty.deviceName &&
      acc.get(msg.deviceName).forall { old =>
        isNewer(old, msg)
      }
    if (mustAdd || acc.isEmpty) { // already checked msg.deviceName
      acc.put(msg.deviceName, msg)
    }
    acc
  }

  override def merge(b1: mutable.Map[String, ReportRawData], b2: mutable.Map[String, ReportRawData]): mutable.Map[String, ReportRawData] = {
    val merged = (b1.keySet ++ b2.keySet).map(k =>
      (b1.get(k), b2.get(k)) match {
        case (Some(v), None) => k -> v
        case (None, Some(v)) => k -> v
        case (Some(old), Some(newer)) if isNewer(old, newer) => k -> old // TODO: Non deterministic
        case (Some(v1), Some(v2)) => k -> v2
      }).toSeq

    mutable.Map(merged: _*)
  }

  override def finish(reduction: mutable.Map[String, ReportRawData]): CompiledReport = {
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
            case (deviceName, data) if deviceName != ReportRawData.empty.deviceName =>
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

  override def bufferEncoder: Encoder[mutable.Map[String, ReportRawData]] =
    ExpressionEncoder[mutable.Map[String, ReportRawData]]

  override def outputEncoder: Encoder[CompiledReport] =
    ExpressionEncoder[CompiledReport]
}
