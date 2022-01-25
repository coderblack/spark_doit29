package cn.doitedu.sparksql

import org.apache.spark.sql.{Encoder, Encoders, SparkSession}
import org.apache.spark.sql.expressions.{Aggregator, UserDefinedFunction}
import org.apache.spark.sql.functions.udaf

object C10_自定义UDAF {
  def main(args: Array[String]): Unit = {
    /**
     * {"id":1,"name":"aa","age":18,"gender":"male"}
     * {"id":2,"name":"bb","age":28,"gender":"male"}
     * {"id":3,"name":"cc","age":38,"gender":"male"}
     * {"id":4,"name":"dd","age":28,"gender":"female"}
     * {"id":5,"name":"ee","age":19,"gender":"female"}
     * {"id":6,"name":"ff","age":20.5,"gender":"female"}
     * {"id":7,"name":"gg","age":26,"gender":"female"}
     *
     * 用sparkSql来实现如下查询：
     * male,38_28
     * female,28_26
     */

    val spark = SparkSession.builder()
      .appName("")
      .master("local")
      .getOrCreate()

    val df = spark.read.json("data/sql/stu.txt")
    df.createTempView("df")

    // 注册自定义udaf函数
    // Aggregator需要转成 UserDefinedFunction ，才能注册
    val function: UserDefinedFunction = udaf(FirstAndSecond)
    spark.udf.register("first_second",function)

    spark.udf.register("my_avg",udaf(MyAverage))

    spark.sql(
      """
        |
        |select
        |   gender,
        |   first_second(age) as first_second,
        |   my_avg(age) as avg_age
        |from df
        |group by gender
        |
        |""".stripMargin).show()
    spark.close()
  }
}
case class Buf(var max:Int,var second:Int)
case class BufAvg(var sum:Int,var cnt:Int)

object FirstAndSecond extends Aggregator[Int, Buf, String] {
  // zero 方法用于对缓存结构进行初始化
  override def zero: Buf = Buf(0,0)

  // 来一条数据 a ，更新一次缓存 b
  // 有大量并行task的做（局部聚合）
  override def reduce(buf: Buf, age: Int): Buf = {
    val lst = List(buf.max, buf.second, age)
    val reverse = lst.sorted.reverse

    Buf(reverse(0),reverse(1))
  }

  // 各个上游task的局部聚合结果，进行全局聚合
  override def merge(b1: Buf, b2: Buf): Buf = {
    val lst = List(b1.max, b1.second, b2.max, b2.second)
    val reverse = lst.sorted.reverse
    Buf(reverse(0),reverse(1))
  }

  // 返回最终结果
  override def finish(reduction: Buf): String = reduction.max+"_"+reduction.second

  override def bufferEncoder: Encoder[Buf] = Encoders.product

  override def outputEncoder: Encoder[String] = Encoders.STRING
}

object MyAverage extends  Aggregator[Int,BufAvg,Double]{
  override def zero: BufAvg = BufAvg(0,0)

  override def reduce(buf: BufAvg, age: Int): BufAvg = {
    BufAvg(buf.sum+age,buf.cnt+1)
  }

  override def merge(b1: BufAvg, b2: BufAvg): BufAvg = {
    BufAvg(b1.sum+b2.sum,b1.cnt+b2.cnt)
  }

  override def finish(buf: BufAvg): Double = buf.sum/buf.cnt.toDouble

  override def bufferEncoder: Encoder[BufAvg] = Encoders.product

  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}