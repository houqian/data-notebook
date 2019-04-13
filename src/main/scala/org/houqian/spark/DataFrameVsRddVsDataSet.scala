package org.houqian.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author : houqian
  * @version : 1.0
  * @since : 2019-04-09
  */
object DataFrameVsRddVsDataSet extends App {


  val spark = SparkSession
    .builder()
    .appName("DataFrameVsRddVsDataSet")
    .master("local[4]")
    .getOrCreate()

  val data = spark.range(1000000000)

  val startTime = System.currentTimeMillis()
  // cost 1165 mills.
   data.count()

  // cost 10214 mills.
  //data.rdd.count()

  val endTime = System.currentTimeMillis()


  println(s"cost ${endTime - startTime} mills.")

  System.in.read()

  spark.stop()


  /*
  val conf = new SparkConf().setMaster("local[4]").setAppName("DataFrameVsRddVsDataSet")
  val sparkContext = SparkContext.getOrCreate(conf)

  val startTime = System.currentTimeMillis()
  // cost 6184 mills.
  sparkContext.parallelize(0.to(1000000000)).count()
  val endTime = System.currentTimeMillis()
  println(s"cost ${endTime - startTime} mills.")

  System.in.read()

  sparkContext.stop()
   */

}
