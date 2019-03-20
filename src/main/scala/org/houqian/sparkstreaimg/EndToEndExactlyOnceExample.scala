package org.houqian.sparkstreaimg

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010._

object EndToEndExactlyOnceExample extends App {

  val conf = new SparkConf().setMaster("local[4]").setAppName("EndToEndExactlyOnceExample")
  conf.registerKryoClasses(Array(classOf[org.apache.kafka.clients.consumer.ConsumerRecord[String, String]]))
  val ssc = new StreamingContext(conf, Seconds(5))

  val kafkaConsumerParams = Map[String, Object](
    "bootstrap.servers" -> "localhost:19092,localhost:29092,localhost:39092",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "sparkstreming-EndToEndExactlyOnceExample",
    "auto.offset.reset" -> "earliest",
    //    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )

  val topics = Array("test-topic")

  val offsets = Map[TopicPartition, Long](
    new TopicPartition(topics(0), 0) -> 0L
  )

  val stream = KafkaUtils.createDirectStream[String, String](
    ssc,
    LocationStrategies.PreferConsistent,
    ConsumerStrategies.Subscribe[String, String](topics, kafkaConsumerParams, offsets)
  )

  /**
    * foreachRDD ： 遍历RDD，而不是
    */
  stream.foreachRDD {
    rdd =>

      def commitRecordAsync(record: ConsumerRecord[String, String]) = {
        //                                                                                      以这个endoffset参数为准
        val commitOne = OffsetRange.create(record.topic(), record.partition(), record.offset(), record.offset())
        stream.asInstanceOf[CanCommitOffsets].commitAsync(Array(commitOne))
      }

      rdd.foreachPartition {
        p =>
          p.foreach {
            record =>
              println(s"${record.key()} --> ${record.value()}")
              commitRecordAsync(record)
          }
      }

    //      stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
  }

  ssc.start()
  ssc.awaitTermination()
}
