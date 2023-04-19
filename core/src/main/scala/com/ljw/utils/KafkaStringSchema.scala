package com.ljw.utils

import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.streaming.connectors.kafka.{KafkaDeserializationSchema, KafkaSerializationSchema}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord

import java.lang
import java.nio.charset.StandardCharsets

class KafkaStringSchema(topic: String) extends KafkaDeserializationSchema[String] with KafkaSerializationSchema[String] {
  override def isEndOfStream(nextElement: String): Boolean = false

  override def deserialize(record: ConsumerRecord[Array[Byte], Array[Byte]]): String = {
    if (record == null || record.value() == null)
      return ""
    else
      return new String(record.value())
  }

  override def serialize(element: String, timestamp: lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
    new ProducerRecord[Array[Byte], Array[Byte]](topic, element.getBytes(StandardCharsets.UTF_8))

  }

  override def getProducedType: TypeInformation[String] = BasicTypeInfo.STRING_TYPE_INFO
}
