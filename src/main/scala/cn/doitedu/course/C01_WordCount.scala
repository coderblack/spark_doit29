package cn.doitedu.course

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object C01_WordCount {
  def main(args: Array[String]): Unit = {

    // 代码中临时设置日志打印级别
    Logger.getLogger("org").setLevel(Level.WARN)

    val conf = new SparkConf()
    // 设置spark的运行模式为local（ yarn）
    conf.setMaster("local")
    // 设置本应用application的名称
    conf.setAppName("wordCount")

    // 创建一个spark的编程入口
    val sc = new SparkContext(conf)

    // 第一步：创建一个 “数据源” 的  RDD（“迭代器”）
    val rdd1: RDD[String] = sc.textFile("data/wordcount/input/wc.txt", 2)
    // rdd.foreach(println)

    // 切单词并打散
    val rdd2 = rdd1.flatMap(s => s.split("\\s+"))

    // 把单词变成  (单词,1)的kv元组
    val rdd3 = rdd2.map(s => (s, 1))

    // 分组聚合
    val rdd4 = rdd3.reduceByKey((acc, ele) => acc + ele)

    // 输出结果
    // rdd4.foreach(println)  // 输出到控制台
    rdd4.saveAsTextFile("data/wordcount/output")

    sc.stop()

  }
}
