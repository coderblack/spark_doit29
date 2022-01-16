package cn.doitedu.course

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

import scala.util.{Random, hashing}

object C12_RDD算子_sample抽样 {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf = new SparkConf
    conf.setAppName("")
    conf.setMaster("local")

    val sc = new SparkContext(conf)


    //val rdd1 = sc.makeRDD(Seq(1, 1, 1, 3, 2, 11, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 2, 1, 1, 1, 1, 1, 1, 3, 3, 1, 1, 1, 3, 1, 2, 2, 2))
    val rdd1 = sc.makeRDD(Seq(10,20, 30, 40, 50, 60),2)

    // 对rdd1抽样
    val sampleRdd: RDD[Int] = rdd1.sample(false, 0.1)
    sampleRdd.foreach(println)


    // 分析，rdd1中哪些值比较多
    val arr: Array[(Int, Int)] = sampleRdd.groupBy(ele => ele).map(tp => (tp._1, tp._2.size)).sortBy(tp => -tp._2).take(2)
    println(arr.mkString(","))


    sc.stop()
  }
}
