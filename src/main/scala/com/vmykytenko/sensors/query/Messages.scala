package com.vmykytenko.sensors.query

import org.apache.kafka.common.serialization.{Deserializer, Serializer}

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}


case class SensorReportRequest(environmentName: String)

object SensorReportRequestDeserializer extends Deserializer[SensorReportRequest] {
  override def deserialize(topic: String, bytes: Array[Byte]): SensorReportRequest = {
    val byteIn = new ByteArrayInputStream(bytes)
    val objIn = new ObjectInputStream(byteIn)
    val obj = objIn.readObject().asInstanceOf[SensorReportRequest]
    byteIn.close()
    objIn.close()
    obj
  }
}

object SensorReportRequestSerializer extends Serializer[SensorReportRequest] {
  override def serialize(topic: String, data: SensorReportRequest): Array[Byte] = {
    try {
      val byteOut = new ByteArrayOutputStream()
      val objOut = new ObjectOutputStream(byteOut)
      objOut.writeObject(data)
      objOut.close()
      byteOut.close()
      byteOut.toByteArray
    }
    catch {
      case ex: Exception => throw new Exception(ex.getMessage)
    }
  }
}

case class SensorReportRequestExt(environmentName: String,
                                  respondTo: String,
                                  report: Array[Byte])

case class SensorReportItem(environmentName: String,
                            deviceName: String,
                            metric: String,
                            value: Double,
                            timestamp: Long)

case class SensorReport(items: Array[SensorReportItem])

object SensorReportSerializer extends Serializer[SensorReport] {
  override def serialize(topic: String, data: SensorReport): Array[Byte] = {
    try {
      val byteOut = new ByteArrayOutputStream()
      val objOut = new ObjectOutputStream(byteOut)
      objOut.writeObject(data)
      objOut.close()
      byteOut.close()
      byteOut.toByteArray
    }
    catch {
      case ex: Exception => throw new Exception(ex.getMessage)
    }
  }
}

object SensorReportDeserializer extends Deserializer[SensorReport] {
  override def deserialize(topic: String, bytes: Array[Byte]): SensorReport = {
    val byteIn = new ByteArrayInputStream(bytes)
    val objIn = new ObjectInputStream(byteIn)
    val obj = objIn.readObject().asInstanceOf[SensorReport]
    byteIn.close()
    objIn.close()
    obj
  }
}

