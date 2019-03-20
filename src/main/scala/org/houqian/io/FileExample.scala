package org.houqian.io

import java.io.File

import scala.io.Source

object FileExample extends App {
  //   read lines from a log file
  Source.fromFile("C:\\Users\\houqi\\IdeaProjects\\spark-notebook\\data\\access.log").getLines().take(10).foreach(println(_))

}
