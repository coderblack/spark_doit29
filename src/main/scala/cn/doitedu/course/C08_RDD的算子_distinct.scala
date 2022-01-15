package cn.doitedu.course

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object C08_RDD的算子_distinct {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("去重")

    val sc = new SparkContext(conf)

    /**
     * 对单词去重
     */
    val rdd1 = sc.textFile("data/wordcount/input/wc.txt")

    val words: RDD[String] = rdd1.flatMap(_.split("\\s+"))

    // 去重
    val res = words.distinct

    res.foreach(println)


    sc.stop()



  }
}
