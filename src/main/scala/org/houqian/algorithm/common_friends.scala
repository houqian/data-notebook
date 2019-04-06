package org.houqian.algorithm

import org.apache.spark.{SparkConf, SparkContext}

/**
  *<pre>
  * 场景：
  * 社交网站上经常会推荐给你，你与你某个好友有多少个共同好友。
  * 如果是你每次点击该好友页面都计算一次的话，代价比较高，一般都是
  * 定时计算好放到缓存（比如redis），每次查缓存即可。
  *
  * 实验：
  * 给定一个文件：
  * uid, friend list
  * ----------------
  * 600, 400 200 100
  * 500, 300 100 900
  * 300, 200 100 450
  * 1000, 600 500 200
  * 要求找出两两之间的共同好友数量
  * </pre>
  *
  * @author : houqian
  * @version : 1.0
  * @since : 2019-04-02
  */
object common_friends {

  val conf = new SparkConf().setMaster("local[4]").setAppName("EndToEndExactlyOnceExample")
  val sc = SparkContext.getOrCreate(conf)

  val data = sc.textFile("/Users/houqian/repo/github/spark-notebook/src/main/scala/org/houqian/algorithm/data.txt")

  data.flatMap {
    item =>
      val tokens = item.split(",")
      val user = tokens(0)
      val sortedFriends = tokens(1).split(" ").map(_.toLong).sortWith(_.compareTo(_) < 0)
      sortedFriends
  }



}
