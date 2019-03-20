package org.houqian.kafka.exactlyonce

import java.time.Duration
import java.util.{Collections, Locale, Properties}

import org.apache.kafka.clients.consumer.ConsumerConfig._
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.requests.IsolationLevel._
import org.apache.kafka.common.serialization.StringDeserializer

import scala.collection.JavaConversions._

object ScalaExactlyOnceConsumerExample extends App {
  val props = new Properties()
  props.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:19092")
  props.put(GROUP_ID_CONFIG, "test-kafka1121")
  props.put(KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])
  props.put(VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])
  props.put(ENABLE_AUTO_COMMIT_CONFIG, false: java.lang.Boolean)
  props.put(AUTO_OFFSET_RESET_CONFIG, "earliest")


  //  消费者从broker拉取消息，消息的隔离级别设置为committed，即仅读取producer已经提交的消息
  props.put(ISOLATION_LEVEL_CONFIG, READ_COMMITTED.toString.toLowerCase(Locale.ROOT))

  val topic = "test-topic"
  val kafkaConsumer = new KafkaConsumer[String, String](props)
  kafkaConsumer.subscribe(Collections.singleton(topic))

  sys.addShutdownHook {
    kafkaConsumer.close()
  }

  while(true) {
    for(record <- kafkaConsumer.poll(Duration.ofMillis(10)).iterator()) {
      println(record)
      Thread.sleep(30L)
    }
    kafkaConsumer.commitSync()
  }
}
