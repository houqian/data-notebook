package org.houqian.spark.sparksql.window

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

/**
  * <pre>
  * spark sql也支持类似hive的窗口函数，但个人测试下来没有hive支持的好。
  * 不过hive on spark可以鱼和熊掌兼得了。其实底层函数都差不多，只不过hive的解释器更成熟一些罢了。
  * </pre>
  * @author : houqian
  * @version : 1.0
  * @since : 2019-04-13
  */
object WindowExample extends App {

  case class Salary(depName: String, empNo: Long, name: String, salary: Long, hobby: Seq[String])

  val spark: SparkSession = SparkSession
    .builder()
    .appName("WindowExample")
    .master("local[4]")
    .enableHiveSupport()
    .getOrCreate()


  // 开启隐式转换以启动sparksql对scala集合的增强
  import spark.implicits._

  val empsalary: Dataset[Salary] = Seq(
    Salary("sales", 1, "Alice", 5000, List("game", "ski")),
    Salary("personnel", 2, "Olivia", 3900, List("game", "ski")),
    Salary("sales", 3, "Ella", 4800, List("skate", "ski")),
    Salary("sales", 4, "Ebba", 4800, List("game", "ski")),
    Salary("personnel", 5, "Lilly", 3500, List("climb", "ski")),
    Salary("develop", 7, "Astrid", 4200, List("game", "ski")),
    Salary("develop", 8, "Saga", 6000, List("kajak", "ski")),
    Salary("develop", 9, "Freja", 4500, List("game", "kajak")),
    Salary("develop", 10, "Wilma", 5200, List("game", "ski")),
    Salary("develop", 11, "Maja", 5200, List("game", "farming"))).toDS
  empsalary.createTempView("empsalary")
  empsalary.show()


  val overCategory: WindowSpec = Window.partitionBy('depName)
  /*
  下面这一坨基本等价于：
   val sql:String = """
    | select
    |   collect_list(salary) as salaries, /*代替group_concat, group_concat返回每个组内的所有元素*/
    |   avg(salary) as average_salary,
    |   sum(salary) as total_salary
    | from
    |   empsalary
    | group by
    |   depName
  """.stripMargin
  只不过解除了group by的限制，可以在select后跟分组中没有的字段
   */
  val df: DataFrame = empsalary
    .withColumn("salaries", collect_list('salary) over overCategory)
    .withColumn("average_salary", (avg('salary) over overCategory).cast("int"))
    .withColumn("total_salary", sum('salary) over overCategory)
    .select("depName", "empNo", "name", "salary", "salaries", "average_salary", "total_salary")
  df.show(false)


  val overCategory2: WindowSpec = Window.partitionBy('depName).orderBy('salary desc)
  /*
  下面这一坨基本等价于：
  val sql:String = """
                     | select
                     |   sort_array(collect_list(salary), false) as sorted_salaries, /*代替group_concat，sort_array方法第二个参数默认值：asc=true（从小到大排序）*/
                     |   cast(avg(salary) as integer) as average_salary,
                     |   sum(salary) as total_salary
                     | from
                     |   empsalary
                     | group by
                     |   depName
                   """.stripMargin
   */
  val df2: DataFrame = empsalary
    .withColumn("salaries", collect_list('salary) over overCategory2)
    .withColumn("average_salary", (avg('salary) over overCategory2).cast("int"))
    .withColumn("total_salary", sum('salary) over overCategory2)
    .select("depName", "empNo", "name", "salary", "salaries", "average_salary", "total_salary")
  df2.show(false)


  val overCategory3: WindowSpec = Window.partitionBy('depName).orderBy('salary desc)
  val df3: DataFrame = empsalary
    .withColumn("salaries", collect_list('salary) over overCategory3)
    .withColumn("rank", rank() over overCategory3)
    .withColumn("dense_rank", dense_rank() over overCategory3)
    .withColumn("row_number", row_number() over overCategory3)
    .withColumn("ntile", ntile(3) over overCategory3)
    .withColumn("percent_rank", percent_rank() over overCategory3)
    .select("depName", "empNo", "name", "salary", "salaries", "rank", "dense_rank", "row_number", "ntile", "percent_rank")
  df3.show(false)
  /*
+---------+-----+------+------+------------------------------+----+----------+----------+-----+------------+
|depName  |empNo|name  |salary|salaries                      |rank|dense_rank|row_number|ntile|percent_rank|
+---------+-----+------+------+------------------------------+----+----------+----------+-----+------------+
|develop  |8    |Saga  |6000  |[6000]                        |1   |1         |1         |1    |0.0         |
|develop  |10   |Wilma |5200  |[6000, 5200, 5200]            |2   |2         |2         |1    |0.25        |
|develop  |11   |Maja  |5200  |[6000, 5200, 5200]            |2   |2         |3         |2    |0.25        |
|develop  |9    |Freja |4500  |[6000, 5200, 5200, 4500]      |4   |3         |4         |2    |0.75        |
|develop  |7    |Astrid|4200  |[6000, 5200, 5200, 4500, 4200]|5   |4         |5         |3    |1.0         |
|sales    |1    |Alice |5000  |[5000]                        |1   |1         |1         |1    |0.0         |
|sales    |3    |Ella  |4800  |[5000, 4800, 4800]            |2   |2         |2         |2    |0.5         |
|sales    |4    |Ebba  |4800  |[5000, 4800, 4800]            |2   |2         |3         |3    |0.5         |
|personnel|2    |Olivia|3900  |[3900]                        |1   |1         |1         |1    |0.0         |
|personnel|5    |Lilly |3500  |[3900, 3500]                  |2   |2         |2         |2    |1.0         |
+---------+-----+------+------+------------------------------+----+----------+----------+-----+------------+
   */
  val sql:String = """
                     | select
                     |   sort_array(collect_list(salary), false) as sorted_salaries, /*代替group_concat，sort_array方法第二个参数默认值：asc=true（从小到大排序）*/
                     |   rank() over (partition by depName order by salary desc) as rank, /*在排序后的分组中，该值的排名*/
                     |   dense_rank() over (partition by depName order by salary desc) as rank,
                     |   row_number() over (partition by depName order by salary desc) as row_number,
                     |   percent_rank() over (partition by depName order by salary desc) as percent_rank,
                     | from
                     |   empsalary
                     | group by
                     |   depName
                   """.stripMargin
  empsalary.sqlContext.sql(sql).show(false)

}


