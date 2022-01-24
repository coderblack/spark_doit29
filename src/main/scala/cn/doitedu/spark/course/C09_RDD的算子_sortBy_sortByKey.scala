package cn.doitedu.spark.course

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object C09_RDD的算子_sortBy_sortByKey {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("去重")

    val sc = new SparkContext(conf)

    /**
     * 对单词去重
     */
    val rdd1: RDD[String] = sc.textFile("data/wordcount/input/wc.txt")

    val words: RDD[String] = rdd1.flatMap(_.split("\\s+"))

    // 统计每个单词的个数，并按单词的个数倒序排序
    val wcount: RDD[(String, Int)] = words.map((_, 1)).reduceByKey(_ + _)

    // 对上面的单词个数统计结果按次数倒序排序
    val res1 = wcount.sortBy(tp => -tp._2)

    // 对上面的单词个数统计结果按次数升序排序
    val res2 = wcount.sortBy(_._2)

    // 对砂锅面的单词个数统计结果按 单词升序排序
    val res3 = wcount.sortBy(_._1)

    // 这次用sortByKey
    val res4 = wcount.sortByKey(false)   // 按单词降序排序

    // 把（单词，次数） 映射成  （次数，单词）,再sortByKey就是按次数排序了
    val res5 = wcount.map(tp=>(tp._2,tp._1)).sortByKey(true)  // 按单词次数排序

    sc.stop()

  }
}
