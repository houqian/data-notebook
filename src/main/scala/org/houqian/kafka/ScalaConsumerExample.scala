package org.houqian.kafka

import java.util.Properties

import org.apache.kafka.clients.consumer.ConsumerConfig.{AUTO_OFFSET_RESET_CONFIG, ENABLE_AUTO_COMMIT_CONFIG}
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.houqian.kafka.exactlyonce.ScalaExactlyOnceConsumerExample.props

import scala.collection.JavaConverters._


object ScalaConsumerExample extends App {

  val props = new Properties()
  props.put("bootstrap.servers", "localhost:39092")
  props.put("group.id", "test-kafka1211")
  props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put(ENABLE_AUTO_COMMIT_CONFIG, false: java.lang.Boolean)
  props.put(AUTO_OFFSET_RESET_CONFIG, "earliest")


  val consumer = new KafkaConsumer[String, String](props)
  val topic = "test-topic"

  consumer.subscribe(java.util.Collections.singletonList(topic))

  while (true) {
    val records = consumer.poll(1000)
    records.asScala.foreach(println(_))
  }



  consumer.close()
}
