package cn.doitedu.course

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object C21_闭包引用 {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf(true)
    conf.setMaster("local")
    conf.setAppName("")
    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(Seq(1, 2, 3, 4, 5, 6, 7))

    val lst = new ListBuffer[Int]()  //   lst对象在driver程序中创建

    rdd.map(e => {
      // 算子内部函数中引用 lst
      // 其实是driver中那个lst经过序列化传到executor上后，又被反序列化出来的lst
      // 对算子内部引用的这个lst不论做什么修改，都不会影响到driver端那个lst
      lst += e
      println(lst)
      e
    }).count()

    println(lst)   // ListBuffer()


    /**
     * 能不能利用这个闭包机制来实现map端join ？？？
     */









  }

}
