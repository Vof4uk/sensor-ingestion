package com.vmykytenko.sensors.query

import com.vmykytenko.sensors.{SensorReport, SensorReportItem, SensorReportKey, SensorReportKeySer, SensorReportSer}
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.expressions.Aggregator

import scala.collection.mutable

class ReportAggregator extends Aggregator[ReportRawData, mutable.Map[String, ReportRawData], CompiledReportMessage] {
  override def zero: mutable.Map[String, ReportRawData] = mutable.Map.empty

  override def reduce(acc: mutable.Map[String, ReportRawData], msg: ReportRawData): mutable.Map[String, ReportRawData] = {
    val mustAdd = acc.get(msg.deviceName).forall { old =>
      old.timestamp < msg.timestamp
    }
    if (mustAdd)
      acc.put(msg.deviceName, msg)

    acc
  }

  override def merge(b1: mutable.Map[String, ReportRawData], b2: mutable.Map[String, ReportRawData]): mutable.Map[String, ReportRawData] = {
    val merged = (b1.keySet ++ b2.keySet).map(k =>
      (b1.get(k), b2.get(k)) match {
        case (Some(v), None) => k -> v
        case (None, Some(v)) => k -> v
        case (Some(v1), Some(v2)) if v1.timestamp > v2.timestamp => k -> v1 // TODO: Non deterministic
        case (Some(v1), Some(v2)) => k -> v2
      }).toSeq

    mutable.Map(merged: _*)
  }

  override def finish(reduction: mutable.Map[String, ReportRawData]): CompiledReportMessage = {
    if (reduction.isEmpty) {
      CompiledReportMessage(
        SensorReportKeySer.serialize("", SensorReportKey("")),
        SensorReportSer.serialize("", SensorReport("", Array()))
      )
    }
    else {
      val data = reduction.head._2
      CompiledReportMessage( // todo inject topic here
        key = SensorReportKeySer.serialize("", SensorReportKey(data.environmentName)),
        value = SensorReportSer.serialize("",
          SensorReport(
            cid = data.cid,
            items = reduction.values.map(v => SensorReportItem(
              v.environmentName,
              v.deviceName,
              v.metric,
              v.value,
              v.timestamp
            )).toArray
          ))
      )
    }
  }

  override def bufferEncoder: Encoder[mutable.Map[String, ReportRawData]] =
    ExpressionEncoder[mutable.Map[String, ReportRawData]]

  override def outputEncoder: Encoder[CompiledReportMessage] =
    ExpressionEncoder[CompiledReportMessage]
}
