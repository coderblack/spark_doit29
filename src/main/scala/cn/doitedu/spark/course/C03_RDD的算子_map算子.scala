package cn.doitedu.spark.course

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

case class Student(id:Int,name:String,term:String,score:Double)

object C03_RDD的各类算子 {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setAppName("")
    conf.setMaster("local")

    val sc = new SparkContext(conf)

    // 创建源头rdd
    // 1,zs,doit29,80
    val rdd1: RDD[String] = sc.textFile("data/wordcount/input/wc2.txt")

    //  map算子 , 把原始数据的每一行数据，变成一个字符串数组
    val rdd2: RDD[Array[String]] = rdd1.map(line=>{
      val arr = line.split(",")
      arr
    })

    //  map算子 , 把原始数据的每一行数据，变成一个Student对象
    val rdd3:RDD[Student] = rdd1.map(line=>{
      val arr = line.split(",")
      Student(arr(0).toInt,arr(1),arr(2),arr(3).toDouble)
    })


    //  map算子 , 把原始数据的每一行数据，变成一个HashMap对象
    val rdd4:RDD[Map[String,String]]= rdd1.map(line=>{
      val arr = line.split(",")
      Map("id"->arr(0),"name"->arr(1),"term"->arr(2),"score"->arr(3))
    })

    // map算子 , 把原始数据的每一行数据，变成一个元组
    val rdd5: RDD[(Int, String, String, Double)] = rdd1.map(line=>{
      val arr = line.split(",")
      (arr(0).toInt,arr(1),arr(2),arr(3).toDouble)
    })

    // map算子 , 把原始数据的每一行数据，变成一个“我最聪明"
    val rdd6: RDD[String] = rdd1.map(line => "我最聪明")


  }
}
