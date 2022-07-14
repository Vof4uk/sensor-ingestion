package com.vmykytenko.sensors.collect

import org.apache.kafka.common.serialization.{Deserializer, Serializer}

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

//{
//    "environmentName": "",
//    "deviceName": "",
//    "metric": "",
//    "value": xxxxxx.xx,
//    "timestamp": epoch
//}
case class SensorMessage(environmentName: String,
                         deviceName: String,
                         metric: String,
                         value: Double,
                         timestamp: Long)

object SensorMessageSerializer extends Serializer[SensorMessage] {
  // TODO:Java serialization is bad. Replace with smth better. E.g. protobuf
  override def serialize(topic: String, data: SensorMessage): Array[Byte] = {
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

object SensorMessageDeserializer extends Deserializer[SensorMessage] {
  override def deserialize(topic: String, bytes: Array[Byte]): SensorMessage = {
    val byteIn = new ByteArrayInputStream(bytes)
    val objIn = new ObjectInputStream(byteIn)
    val obj = objIn.readObject().asInstanceOf[SensorMessage]
    byteIn.close()
    objIn.close()
    obj
  }
}

case class SensorMessageKey(environmentName: String,
                            deviceName: String)

object SensorMessageKeyDeserializer extends Deserializer[SensorMessageKey] {
  override def deserialize(topic: String, bytes: Array[Byte]): SensorMessageKey = {
    val byteIn = new ByteArrayInputStream(bytes)
    val objIn = new ObjectInputStream(byteIn)
    val obj = objIn.readObject().asInstanceOf[SensorMessageKey]
    byteIn.close()
    objIn.close()
    obj
  }
}

object SensorMessageKeySerializer extends Serializer[SensorMessageKey] {
  // TODO:Java serialization is bad. Replace with smth better. E.g. protobuf
  override def serialize(topic: String, data: SensorMessageKey): Array[Byte] = {
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