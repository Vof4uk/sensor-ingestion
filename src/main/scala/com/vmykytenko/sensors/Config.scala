package com.vmykytenko.sensors

case class KafkaProducerConfig(topic: String,
                               servers: String)

case class KafkaConsumerConfig(servers: String,
                               topic: String)

case class ApplicationStorageConfig(parquetPath: String,
                                    memorySinkName: String,
                                    checkpointLocation: String)
