package cn.doitedu.spark.course

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object C02_WordCount_2 {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("随便")

    val sc = new SparkContext(conf)

    val rdd1: RDD[String] = sc.textFile("data/wordcount/input/wc2.txt")
    val rdd2: RDD[(String, Double)] = rdd1.map(s => {
      val splits = s.split(",")
      // 班级,分数
      (splits(2),splits(3).toDouble)
    })

    // reduceByKey是对上面的rdd按key分组，然后聚合value
    val rdd3: RDD[(String, Double)] = rdd2.reduceByKey(_ + _)

    rdd3.foreach(println)

    sc.stop()
  }

}
