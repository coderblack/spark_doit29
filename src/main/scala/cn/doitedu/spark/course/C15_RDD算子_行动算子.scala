package cn.doitedu.spark.course

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.sql.DriverManager

/**
 * 所谓行动算子，就是会触发RDD的计算链条的算子
 * 触发计算得到结果数据，然后选择一个去处：  比如 ，
 * 打印/写入数据库
 * 存到文件
 * 收集到一个本地集合
 */
object C15_RDD算子_行动算子 {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf = new SparkConf
    conf.setAppName("")
    conf.setMaster("local")

    val sc = new SparkContext(conf)

    val rdd: RDD[(Int, String, Int)] = sc.makeRDD(Seq(
      (1, "a", 13),
      (2, "b", 23),
      (3, "c", 33),
      (4, "d", 43)
    ))

    // foreach  打印
    rdd.foreach(tp => println(tp))

    // foreach  插入mysql
    rdd.foreach(tp => {
      val conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/abc", "root", "123456")
      val statement = conn.prepareStatement("insert into c values(?,?,?)")
      statement.setInt(1, tp._1)
      statement.setString(2, tp._2)
      statement.setInt(3, tp._3)
      statement.execute()
      statement.close()
      conn.close()
    })


    // foreachPartition
    rdd.foreachPartition(iter => {
      val conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/abc", "root", "123456")
      val statement = conn.prepareStatement("insert into c values(?,?,?)")
      iter.foreach(tp => {
        statement.setInt(1, tp._1)
        statement.setString(2, tp._2)
        statement.setInt(3, tp._3)
        statement.execute()
      })
    })


    // take  从rdd中取n条数据，并返回到一个本地集合里
    val datas1: Array[(Int, String, Int)] = rdd.take(2)
    println(datas1.mkString(":"))

    // collect 将rdd的所有数据，收集到一个本地集合
    // 不要轻易使用collect，除非你很清楚你要收集的rdd所代表数据量很小
    val datas2: Array[(Int, String, Int)] = rdd.collect()


    // saveAsXXXFile
    rdd.saveAsTextFile("data/save/")


    sc.stop()
  }

}
