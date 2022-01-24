package cn.doitedu.spark.course

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object C05_RDD的算子_filter算子 {

  def main(args: Array[String]): Unit = {

    // 从wc3.txt中  过滤掉分数<90的数据


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

    // filter算子，用来对rdd中的数据进行按行过滤  ==> sql:  select * from rdd2 where score>=90
    val rdd3 = rdd2.filter(stu => stu.score >= 90)
    rdd3.foreach(println)


    // 过滤掉rdd2中的doit29期的学员的信息
    val rdd4 = rdd2.filter( stu=> !stu.term.equals("doit29"))


    // 过滤掉rdd2中id<3的数据
    val rdd5 = rdd2.filter(stu => stu.id >= 3)


    sc.stop
  }
}
