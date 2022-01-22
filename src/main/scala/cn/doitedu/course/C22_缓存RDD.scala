package cn.doitedu.course

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

// 1,zs,doit29,80
case class Soldier(id: Int, name: String, term: String, score: Int)

object C22_缓存RDD {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf(false)
    conf.setMaster("local")
    conf.setAppName("")
    val sc = new SparkContext(conf)


    val rdd1: RDD[String] = sc.textFile("data/wordcount/input/wc3.txt")

    val rdd2: RDD[Soldier] = rdd1.flatMap(line => {
      val arr = line.split("\\|")
      arr.map(line => {
        val splits = line.split(",")
        Soldier(splits(0).toInt, splits(1), splits(2), splits(3).toInt)
      })
    })

    // 统计每个班的最高分
    val res1 = rdd2
      .groupBy(soldier => soldier.term)
      .map(tp => {
        (tp._1,tp._2.map(_.score).max)
      })

    // 统计每个班的总分



    // 统计每个班的人数


  }

}
