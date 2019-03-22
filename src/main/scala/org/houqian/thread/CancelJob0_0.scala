package org.houqian.thread

import scala.util.control.Breaks.{break, breakable}

/**
  * 主线程向子线程发送interrupt信号，主动中断子线程任务
  */
object CancelJob0_0 extends App {
  val tasks = Map()
  for (elem <- 1 to 10) {

  }
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
          }
        }
      )
    }
  }
  task.start()

  // 主线程睡一会儿，否则上面线程池还没执行就被cancel了
  Thread.sleep(1000L)
  task.interrupt()
}
