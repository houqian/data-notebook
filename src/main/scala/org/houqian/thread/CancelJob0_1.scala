package org.houqian.thread

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.Calendar

import scala.util.control.Breaks.{break, breakable}

/**
  *
  * 主线程向子线程发送interrupt信号，主动中断子线程任务<br>
  * 并且维护一个set来管理多个线程
  */
object CancelJob0_1 extends App {

  def newTask = new Thread {
    override def run(): Unit = {
      val startTime = System.currentTimeMillis()
      this.setName(s"task_${System.nanoTime()}")
      breakable(
        while (true) {
          try {
            val jobExecTime = 1000 * 30L
            println(s"job start, thread [${this.getName}], need ${jobExecTime}ms.")
            Thread.sleep(jobExecTime)
          }
          catch {
            //  捕获该异常，可以在该任务被cancel时做处理，例如事务回滚
            case e: InterruptedException => println(s"thread [${this.getName}] interrupted, real cost: ${System.currentTimeMillis() - startTime} ms")
              break()
          }
        }
      )
    }
  }

  val threadSet = 0.to(10).map {
    n =>
      val nTask = newTask
      nTask.start()
      nTask
  }.toSet

  threadSet.foreach(println)

  // 主线程睡一会儿，否则上面线程池还没执行就被cancel了
  Thread.sleep(1000L)
  //  这里以停掉第一个任务为例
  val needStopJob = threadSet.toList(0)
  println(needStopJob)
  needStopJob.interrupt()
  val set = threadSet - needStopJob
  if (set.contains(needStopJob))
    println("没删除成功")
  else {
    println("删除成功")
    set.foreach(println)
  }
}
