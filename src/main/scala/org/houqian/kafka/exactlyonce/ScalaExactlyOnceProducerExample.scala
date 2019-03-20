package org.houqian.kafka.exactlyonce

import java.util.Properties

import org.apache.kafka.clients.producer.ProducerConfig._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.KafkaException

import scala.io.Source

object ScalaExactlyOnceProducerExample extends App {
  val props = new Properties()
  props.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:19092,localhost:29092,localhost:39092")
  props.put(CLIENT_ID_CONFIG, "ScalaExactlyOnceExample2")
  props.put(KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  props.put(VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

  //  需要指定这两项使正好一次语义生效
  props.put(ENABLE_IDEMPOTENCE_CONFIG, true: java.lang.Boolean)
  props.put(TRANSACTIONAL_ID_CONFIG, "prod-3") // 确保每个生产者的id不相同
  val topic = "test-topic"

  val producer = new KafkaProducer[String, String](props)
  producer.initTransactions()

  /**
    * 如果程序被异常关闭，则最近一次还没有完成的事务应该被取消掉，否则造成事务始终未提交
    * 需要注意producer.abortTransaction()未执行前producer不可以close，应该将close步骤放在abort后面
    */
  sys.addShutdownHook {
    try {
      //  异常捕获原因：没有提供producer是否被关闭、最近一次事务是否完成的api
      producer.abortTransaction()
    } catch {
      case ke: KafkaException => {
        // 由于transaction state也获取不到，如果是正常状态，先临时这样判定
        if (ke.getMessage.contains("Invalid transition attempted from state READY to state ABORTING_TRANSACTION"))
          println("程序正常结束")
        else
          ke.printStackTrace()
      }
      case e: Exception => e.printStackTrace()
    }

    producer.close()
  }

  val lines = Source.fromFile("C:\\Users\\houqi\\IdeaProjects\\spark-notebook\\data\\access.log").getLines()


  //  按照1000切片，之后每个切片执行一次事务
  for (slideLines <- lines.zipWithIndex.sliding(1000, 1000)) {
    val startOffset = slideLines(0)._2
    val endOffset = slideLines(slideLines.size - 1)._2

    try {
      producer.beginTransaction()
      for ((line, index) <- slideLines) {
        val record = new ProducerRecord[String, String](topic, index.toString, line)
        producer.send(record)
      }
      producer.commitTransaction()
      println(s"[${startOffset}, ${endOffset}] finish tx!")
    } catch {
      case ke: KafkaException => producer.abortTransaction()
      case e: Exception => e.printStackTrace()
    }
  }

  producer.close()

}
