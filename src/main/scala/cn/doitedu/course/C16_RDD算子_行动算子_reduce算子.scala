package cn.doitedu.course

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object C16_RDD算子_行动算子_reduce算子 {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("")

    val sc = new SparkContext(conf)

    val rdd1: RDD[Int] = sc.parallelize(List(1, 2, 3, 4, 5, 10))
    val rdd2 = rdd1.map(e => e * 10)

    // reduce算子，是将整个RDD的所有元素通过传入的函数折叠聚合成一个具体的值
    // 所以，它需要得到rdd中的具体数据，它就是一个行动算子
    val i: Int = rdd2.reduce(_ + _)
    println(i)

    sc.stop()

  }

}
