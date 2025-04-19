// src/main/scala/com/taxi/producer/KafkaProducerWrapper.scala
package com.taxi.producer

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import java.util.Properties

class KafkaProducerWrapper(bootstrapServers: String) {
  
  private val props = new Properties()
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
  props.put(ProducerConfig.ACKS_CONFIG, "1")
  props.put(ProducerConfig.RETRIES_CONFIG, "3")
  props.put(ProducerConfig.LINGER_MS_CONFIG, "5")
  props.put(ProducerConfig.BATCH_SIZE_CONFIG, "16384")
  
  private val producer = new KafkaProducer[String, String](props)
  
  def send(topic: String, key: String, value: String): Unit = {
    val record = new ProducerRecord[String, String](topic, key, value)
    producer.send(record)
  }
  
  def send(topic: String, value: String): Unit = {
    val record = new ProducerRecord[String, String](topic, value)
    producer.send(record)
  }
  
  def flush(): Unit = {
    producer.flush()
  }
  
  def close(): Unit = {
    producer.close()
  }
}