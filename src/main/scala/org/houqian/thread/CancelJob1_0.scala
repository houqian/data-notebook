package org.houqian.thread

import java.util.concurrent.{Executors, ThreadPoolExecutor}

import scala.util.control.Breaks._

/**
  * 提供一个任务提交到线程池执行后，主线程主动取消任务的例子
  */
object CancelJob1_0 extends App {

  val exec = Executors.newSingleThreadExecutor()

  val task = new Thread {
    override def run(): Unit = {
      val startTime = System.currentTimeMillis()

      breakable(
        while (true) {
          try {
            val jobExecTime = 1000 * 30L
            println(s"job start, need ${jobExecTime}ms.")
            Thread.sleep(jobExecTime)
          }
          catch {
            //  捕获该异常，可以在该任务被cancel时做处理，例如事务回滚
            case e: InterruptedException => println(s"interrupted, real cost: ${System.currentTimeMillis() - startTime} ms")
              break()
              throw e
          }
        }
      )
    }
  }

  println("submmit task..")
  val future = exec.submit(task)

  // 主线程睡一会儿，否则上面线程池还没执行就被cancel了
  Thread.sleep(1000L)
  future.cancel(true)

}

