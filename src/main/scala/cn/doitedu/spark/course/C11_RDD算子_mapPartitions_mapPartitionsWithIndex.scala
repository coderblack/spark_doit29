package cn.doitedu.spark.course

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.sql.DriverManager
import java.util.Random

case class Programmer(id: Int, name: String, age: Int, var phone: String = "", var city: String = "")

object C11_RDD算子_mapPartitions_mapPartitionsWithIndex {

  def main(args: Array[String]): Unit = {


    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf = new SparkConf
    conf.setAppName("")
    conf.setMaster("local")

    val sc = new SparkContext(conf)


    val rdd1: RDD[String] = sc.makeRDD(Seq("1,zs,18", "2,ls,28", "3,ww,26", "4,ww,32", "5,hh,32"), 3)


    // map ， 是一次性给你一条数据，让你用函数去加工（映射）
    // rdd1有几条数据，你传入的函数就会被调用几次
    val rdd2: RDD[Programmer] = rdd1.map(line => {
      println("创建连接 ---------")
      // 创建jdbc连接  （每条数据的处理都会创建连接）
      // 提取line中 id
      // 用jdbc去查询id对应的phone和city
      val arr = line.split(",")
      Programmer(arr(0).toInt, arr(1), arr(2).toInt)
    })
    rdd2.count()


    /**
     * mapPartitions，是一次性给你一个分区，让你用函数去加工（映射）
     * rdd1有几个分区，你传入的函数就会被调用几次
     */
    val rdd3 = rdd1.mapPartitions(partitionIterator => {
      val jdbc = "创建jdbc连接 ************" + new Random().nextInt(5)
      println(jdbc)

      val iter2: Iterator[Programmer] = partitionIterator.map(line => {
        println(s"用建好的连接${jdbc} 查询数据")
        // 提取line中 id
        // 用jdbc去查询id对应的phone和city
        val arr = line.split(",")
        Programmer(arr(0).toInt, arr(1), arr(2).toInt)
      })

      iter2
    })
    rdd3.count()


    println("-----------涛哥很爱你们---------------")
    /**
     * mapPartitionsWithIndex
     *   与mapPartitions功能相同，唯一多了一点：  向你传递分区迭代器的同时，还顺便传了分区的分区号给你用
     */
    val tmp = rdd1.mapPartitionsWithIndex((idx,iter)=>{
      iter.map(s=>(idx,s))
    })
    tmp.foreach(println)


    println("-----------涛哥再爱你们一次---------------")



    /**
     * mapPartitions的真实应用
     * 对rdd1的数据映射成Programmer，并且从mysql表中补充“phone” 和 “city”信息
     */
    val rdd4 = rdd1.mapPartitions(iter => {
      // 创建jdbc连接
      val conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/abc", "root", "123456")
      val statement = conn.prepareStatement("select phone,city from programmer where id=?")

      // 处理分区中的数据
      iter.map(line => {
        // "1,zs,18"
        val arr = line.split(",")
        // 去mysql中查询phone和city
        statement.setInt(1, arr(0).toInt)
        val resultSet = statement.executeQuery()

        var phone = ""
        var city = ""
        while (resultSet.next()) {
          phone = resultSet.getString("phone")
          city = resultSet.getString("city")
        }

        Programmer(arr(0).toInt, arr(1), arr(2).toInt, phone, city)
      })
    })

    rdd4.foreach(println)










    sc.stop()
  }
}
