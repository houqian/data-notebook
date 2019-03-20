package org.houqian

/**
 * @author ${user.name}
 */
object App {
  
  def foo(x : Array[String]) = x.foldLeft("")((a,b) => a + b)
  
  def main(args : Array[String]) {
    println( "Hello World!" )
    println("concat arguments = " + foo(args))

    val zimu = List("a", "b", "c", "d", "e")
    val slided2 = zimu.zipWithIndex.sliding(2)
    for (elem <- slided2) {
      for ((k, v) <- elem) {
        println(s"${k} -> ${v}")
      }
    }
  }

}
