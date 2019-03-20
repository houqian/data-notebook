package org.houqian.kafka

import java.util
import java.util.Properties

import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG


object KafkaAdminClientExample {

  val props = new Properties()
  props.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:19092,localhost:29092,localhost:39092")
  val kafkaAdminClient = AdminClient.create(props)

  def main(args: Array[String]): Unit = {
    deleteTopic("test-topic")
  }

  private def deleteTopic(topic: String) = {
    kafkaAdminClient.deleteTopics(util.Collections.singleton(topic));
    print("topics: ")
    println(kafkaAdminClient.listTopics().names())
  }


}
