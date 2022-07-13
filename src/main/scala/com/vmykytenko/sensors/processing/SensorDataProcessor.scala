package com.vmykytenko.sensors.processing

import com.vmykytenko.sensors.messaging.{SensorMessage, SensorMessageDeserializer, SensorMessageKey, SensorMessageKeyDeserializer}
import org.apache.spark.SparkContext
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}

class SensorDataProcessor {
  def apply(sparkContext: SparkContext,
            kafkaInParams: Map[String, Object]) = {
    val kafkaParams = kafkaInParams ++ Map[String, Object](
      "bootstrap.servers" -> "localhost:9092,anotherhost:9092",
      "key.deserializer" -> classOf[SensorMessageKeyDeserializer],
      "value.deserializer" -> classOf[SensorMessageDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )


    val streamingContext = new StreamingContext(sparkContext, Seconds(1))

    val topics = Array("topicA", "topicB")

    val stream = KafkaUtils.createDirectStream[SensorMessageKey, SensorMessage](
      streamingContext,
      PreferConsistent,
      Subscribe[SensorMessageKey, SensorMessage](topics, kafkaParams)
    )

    stream.map(record => (record.key, record.value))
  }
}
