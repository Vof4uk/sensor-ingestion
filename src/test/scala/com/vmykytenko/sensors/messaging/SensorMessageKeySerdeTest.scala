package com.vmykytenko.sensors.messaging

import com.vmykytenko.sensors.collect.{SensorMessageKey, SensorMessageKeyDeserializer, SensorMessageKeySerializer}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

class SensorMessageKeySerdeTest extends AnyFlatSpec with should.Matchers {
  behavior of "SensorMessage Serialization"

  it should "Serialize and Deserialize a Sensor Message" in {
    val expected =
      SensorMessageKey("myEnv-1", "a thermometer-2")

    val serialized =
      SensorMessageKeySerializer.serialize("topic-1", expected)

    val deserialized =
      SensorMessageKeyDeserializer.deserialize("topic-1", serialized)

    deserialized shouldEqual expected

  }
}
