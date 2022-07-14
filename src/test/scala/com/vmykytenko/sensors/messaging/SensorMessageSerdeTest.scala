package com.vmykytenko.sensors.messaging

import com.vmykytenko.sensors.collect.{SensorMessage, SensorMessageDeserializer, SensorMessageSerializer}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

class SensorMessageSerdeTest extends AnyFlatSpec with should.Matchers {
  behavior of "SensorMessage Serialization"

  it should "Serialize and Deserialize a Sensor Message" in {
    val expected =
      SensorMessage("myEnv", "a lonely humidity meter", "humidity", 0.6856, 1657724196)

    val serialized =
      SensorMessageSerializer.serialize("topic-1", expected)

    val deserialized =
      SensorMessageDeserializer.deserialize("topic-1", serialized)

    deserialized shouldEqual expected

  }
}
