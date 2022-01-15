package cn.doitedu.exercise

import cn.doitedu.course.Student
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object X01_分组聚合练习 {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    // 针对wc3.txt这个数据，统计如下需求
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("")

    val sc = new SparkContext(conf)

    val rdd1: RDD[Student] = sc
      .textFile("data/wordcount/input/wc3.txt")
      .flatMap(line => {
        line.split("\\|").map(s => {
          val fields = s.split(",")
          Student(fields(0).toInt, fields(1), fields(2), fields(3).toDouble)
        })
      })

    /**
     * 需求1： 每个班级的平均成绩
     */
    val res1 = rdd1
      .groupBy(stu=>stu.term)
      .map(tp=>{
        val cnt = tp._2.size
        val amount = tp._2.map(_.score).sum
        (tp._1,amount/cnt)
      })
    res1.foreach(println)


    println("----------------------涛哥累了--------------------")

    /**
     * 需求2： 每个班级分数最高的学员信息
     */
    val res2 = rdd1
      .groupBy(_.term)
      .map(tp=>{
        val student = tp._2.maxBy(_.score)
        (tp._1,student)
      })

    res2.foreach(println)


    sc.stop()
  }
}
