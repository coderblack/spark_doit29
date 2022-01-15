package cn.doitedu.exercise

import cn.doitedu.course.Student
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object X02_排序练习 {
  def main(args: Array[String]): Unit = {

    /**
     * 需求1：  对wc3.txt中的学生信息进行去重（相同的人只留下一个）
     * 需求2：  对wc3.txt中的学生信息，按 分数 降序排序
     * 需求3：  统计各班的总分，并按总分大小降序排序输出
     *
     */
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("")

    val sc = new SparkContext(conf)

    // 读原始数据
    val rdd1: RDD[String] = sc.textFile("data/wordcount/input/wc3.txt")

    // 转换为 RDD[Student]
    val rdd2: RDD[Student] = rdd1.flatMap(line => {
      val arr = line.split("\\|")
      arr.map(s => {
        val fields = s.split(",")
        Student(fields(0).toInt, fields(1), fields(2), fields(3).toDouble)
      })
    })

    // 1. 去重
    val distinctedStudents = rdd2.distinct
    distinctedStudents.foreach(println)


    // 2. 按分数降序排序
    val sortedByScore = distinctedStudents.sortBy(stu => -stu.score)
    sortedByScore.foreach(println)


    // 3. 统计个班总分，并按总分降序排序
    val sortedAmount = distinctedStudents
      .groupBy(stu => stu.term)
      .map(tp => {
        val term = tp._1
        val amount = tp._2.map(stu => stu.score).sum
        (term, amount)
      })
      .sortBy(tp=> -tp._2)
    sortedAmount.foreach(println)


    sc.stop
  }

}
