package cn.doitedu.course

import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

import java.sql.{DriverManager, ResultSet}


case class Per(id: Int, age: Int, name: String, score: Int)

object C17_RDD算子_映射数据源的算子 {

  def main(args: Array[String]): Unit = {


    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("")
    conf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")

    val sc = new SparkContext(conf)


    // 将文件映射成RDD
    val rdd1 = sc.textFile("data/wordcount/wc2.txt")
    // rdd1 的内部封装的迭代器是一个 ：读文件的迭代器


    val rdd2 = sc.makeRDD(Seq(1, 2, 3, 4, 5, 6))
    // rdd2 的内部封装的迭代器是一个： 集合的迭代器

    // 创建数据库连接的逻辑封装
    val getConn = () => {
      DriverManager.getConnection("jdbc:mysql://localhost:3306/abc", "root", "123456")
    }

    // 将一行数据库查询结果变成我们自己所要的类型的逻辑封装
    val mapRow = (resultSet:ResultSet) => {
      val id = resultSet.getInt(1)
      val age = resultSet.getInt(2)
      val name = resultSet.getString(3)
      val score = resultSet.getInt(4)

      Per(id,age,name,score)
    }


    val rdd3 = new JdbcRDD[Per](sc, getConn, "select * from stu where id>=?  and id<=?", 1, 3, 2,mapRow)
    rdd3.foreach(println)


  }

}
