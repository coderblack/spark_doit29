package cn.doitedu.course

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object C04_RDD的各类算子_flatmap算子 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setAppName("")
    conf.setMaster("local")

    val sc = new SparkContext(conf)

    val rdd1 = sc.textFile("data/wordcount/input/wc3.txt")

    // 1,zs,doit29,80|5,aa,doit30,95|9,ee,doit30,100
    // 步骤1： 把字符串映射成  List[Student]
    val rdd2: RDD[List[Student]] = rdd1.map(line => {
      val arr = line.split("\\|")
      val studentArr = arr.map(s => {
        val fields = s.split(",")
        Student(fields(0).toInt, fields(1), fields(2), fields(3).toDouble)
      })
      studentArr.toList
    })

    rdd2.foreach(println)

    /**
     * List(Student(1,zs,doit29,80.0), Student(5,aa,doit30,95.0), Student(9,ee,doit30,100.0))
     * List(Student(2,ls,doit29,90.0), Student(6,bb,doit30,90.0))
     * List(Student(3,ww,doit30,70.0), Student(7,cc,doit29,80.0))
     * List(Student(4,zl,doit30,95.0), Student(8,dd,doit29,90.0))
     */

    // 步骤2： 把List[Student] 打散成  Student
    val rdd3 = rdd2.flatMap(lst => lst)
    // rdd3.foreach(println)



    /**
     * 直接从rdd1用flatMap就能实现上面两个步骤实现的效果
     */

    // 1,zs,doit29,80|5,aa,doit30,95|9,ee,doit30,100
    val rdd4: RDD[Student] = rdd1.flatMap(line=>{
      val arr = line.split("\\|")
      val studentArr = arr.map(s=>{
        val fields = s.split(",")
        Student(fields(0).toInt,fields(1),fields(2),fields(3).toDouble)
      })

      studentArr
    })
    rdd4.foreach(println)















    sc.stop()
  }

}
