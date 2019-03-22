package org.houqian.thread

import java.util.concurrent.{Executors, ThreadPoolExecutor}

import scala.util.control.Breaks._

/**
  * 提供一组任务提交到线程池执行后，主线程主动取消任务的例子
  */
object CancelJob1_1 extends App {

  val exec = Executors.newFixedThreadPool(20)

  def newTask = new Thread {
    override def run(): Unit = {
      val startTime = System.currentTimeMillis()
      Thread.currentThread().setName(s"task_${System.nanoTime()}")

      breakable(
        while (true) {
          try {
            val jobExecTime = 1000 * 30L
            println(s"job start, thread [${Thread.currentThread().getName}], need ${jobExecTime}ms.")
            Thread.sleep(jobExecTime)
          }
          catch {
            //  捕获该异常，可以在该任务被cancel时做处理，例如事务回滚
            case e: InterruptedException => println(s"thread [${Thread.currentThread().getName}] interrupted, real cost: ${System.currentTimeMillis() - startTime} ms")
              break()
          }
        }
      )
    }
  }

  val futureTaskSet = 1.to(10).map {
    n =>
      println("submmit task..")
      val nTask = newTask
      val future = exec.submit(nTask)
      (future, nTask)
  }.toSet


  // 主线程睡一会儿，否则上面线程池还没执行就被cancel了
  Thread.sleep(1000L)
  //  这里以停掉第一个任务为例
  val needStopTask = futureTaskSet.toList(0)
  needStopTask._1.cancel(true)
  val fs = futureTaskSet - needStopTask

  fs.foreach(ft => println(s"${ft._1} -> ${ft._2.getName}"))

}

