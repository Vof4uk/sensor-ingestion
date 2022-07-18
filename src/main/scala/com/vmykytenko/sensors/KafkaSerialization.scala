package com.vmykytenko.sensors

import org.apache.kafka.common.serialization.{Deserializer, Serializer}
import org.json4s.{DefaultFormats}
import org.json4s.jackson.Serialization.{read, write}
import JsonKafkaSerialization._

case object JsonKafkaSerialization {
  implicit val formats = DefaultFormats
}

case object SensorMessageSer extends Serializer[SensorMessage] {
  override def serialize(topic: String, data: SensorMessage): Array[Byte] =
    write(data).getBytes()
}

case object SensorMessageDe extends Deserializer[SensorMessage] {
  override def deserialize(topic: String, data: Array[Byte]): SensorMessage =
    read[SensorMessage](new String(data))
}


case object SensorMessageKeySer extends Serializer[SensorMessageKey] {
  override def serialize(topic: String, data: SensorMessageKey): Array[Byte] =
    write(data).getBytes()
}

case object SensorMessageKeyDe extends Deserializer[SensorMessageKey] {
  override def deserialize(topic: String, data: Array[Byte]): SensorMessageKey =
    read[SensorMessageKey](new String(data))
}

case object SensorReportRequestSer extends Serializer[SensorReportRequest]{
  override def serialize(topic: String, data: SensorReportRequest): Array[Byte] =
    write(data).getBytes()
}

case object SensorReportRequestDe extends Deserializer[SensorReportRequest] {
  override def deserialize(topic: String, data: Array[Byte]): SensorReportRequest =
    read[SensorReportRequest](new String(data))
}

case object SensorReportRequestKeySer extends Serializer[SensorReportRequestKey] {
  override def serialize(topic: String, data: SensorReportRequestKey): Array[Byte] =
    write(data).getBytes()
}

case object SensorReportRequestKeyDe extends Deserializer[SensorReportRequestKey] {
  override def deserialize(topic: String, data: Array[Byte]): SensorReportRequestKey =
    read[SensorReportRequestKey](new String(data))
}

case object SensorReportSer extends Serializer[SensorReport] {
  override def serialize(topic: String, data: SensorReport): Array[Byte] =
    write(data).getBytes()
}

case object SensorReportDe extends Deserializer[SensorReport] {
  override def deserialize(topic: String, data: Array[Byte]): SensorReport =
    read[SensorReport](new String(data))
}

case object SensorReportKeySer extends Serializer[SensorReportKey]{
  override def serialize(topic: String, data: SensorReportKey): Array[Byte] =
    write(data).getBytes()
}

case object SensorReportKeyDe extends Deserializer[SensorReportKey] {
  override def deserialize(topic: String, data: Array[Byte]): SensorReportKey =
    read[SensorReportKey](new String(data))
}
