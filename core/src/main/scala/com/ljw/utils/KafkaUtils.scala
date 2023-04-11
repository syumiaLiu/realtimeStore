package com.ljw.utils

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.connector.kafka.sink.{KafkaRecordSerializationSchema, KafkaRecordSerializationSchemaBuilder, KafkaSink}
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkFixedPartitioner

class KafkaUtils {
  private val bootstrap = "112.124.36.125:9092"
  private val group = "group1"
  def createDataStream(bootstrap: String,topic:String,group: String): KafkaSource[String] = {
      KafkaSource.builder[String]()
        .setBootstrapServers(bootstrap)
        .setTopics(topic)
        .setValueOnlyDeserializer(new SimpleStringSchema())
        .setGroupId(group)
        .setStartingOffsets(OffsetsInitializer.latest())
        .build()
  }
  def createDataStream(topic:String,group: String):  KafkaSource[String] = {
    createDataStream(bootstrap, topic, group)
  }

  def createDataStream(topic: String): KafkaSource[String] = {
    createDataStream(bootstrap,topic,group)
  }

  def createSink(bootstrap: String,topic:String): KafkaSink[String] = {
    KafkaSink.builder[String]()
      .setBootstrapServers(bootstrap)
      .setRecordSerializer(KafkaRecordSerializationSchema.builder[String]()
        .setTopic(topic)
        .setKeySerializationSchema(new SimpleStringSchema())
        .setValueSerializationSchema(new SimpleStringSchema())
        .setPartitioner(new FlinkFixedPartitioner[String]())
        .build()
       )
      .build()

  }

  def createSink(topic: String): KafkaSink[String] = {
    createSink(bootstrap,topic)
  }

}
