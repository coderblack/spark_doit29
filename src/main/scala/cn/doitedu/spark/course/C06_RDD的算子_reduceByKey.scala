package cn.doitedu.spark.course

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object C06_RDD的算子_reduceByKey {

  def main(args: Array[String]): Unit = {
    /**
     * 需求背景： 求每个班的总成绩
     */

    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("")

    val sc = new SparkContext(conf)

    val rdd1 = sc.textFile("data/wordcount/input/wc3.txt")

    val rdd2: RDD[Student] = rdd1.flatMap(line=>{
      val arr = line.split("\\|")
      arr.map(s=>{
        val fields = s.split(",")
        Student(fields(0).toInt,fields(1),fields(2),fields(3).toDouble)
      })
    })


    // 把每一个student变成 （所在班级，自己的分数）
    val rdd3: RDD[(String, Double)] = rdd2.map(stu => (stu.term, stu.score))

    // 分组聚合成：(班级,总分）
    val rdd4: RDD[(String, Double)] = rdd3.reduceByKey(_ + _)


    /**
     *
     */







    sc.stop()
  }
}
