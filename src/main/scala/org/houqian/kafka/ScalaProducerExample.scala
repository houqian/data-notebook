package org.houqian.kafka

import java.io.File
import java.util.Properties
import scala.collection.JavaConverters._

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.io.Source

object ScalaProducerExample extends App {

  val props = new Properties()
  props.put("bootstrap.servers", "localhost:19092,localhost:29092,localhost:39092")
  props.put("group.id", "test-kafka111")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  val producer = new KafkaProducer[String, String](props)

  val topic = "test-topic"
//  val record = new ProducerRecord[String, String](topic, "test", "test")
//  producer.send(record)
  for ((line, index) <- Source.fromFile("C:\\Users\\houqi\\IdeaProjects\\spark-notebook\\data\\access.log").getLines().zipWithIndex) {
    val record = new ProducerRecord[String, String](topic, index.toString, line)
    println("send ------")
    producer.send(record)
  }

  producer.close()
  println("sending data all done.")


}


