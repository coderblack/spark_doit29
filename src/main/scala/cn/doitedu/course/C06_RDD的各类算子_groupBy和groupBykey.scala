package cn.doitedu.course

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object C06_RDD的各类算子_groupBy和groupBykey {
  def main(args: Array[String]): Unit = {

    /**
     * 需求背景： 求每个班的总成绩
     */

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

    /**
     *   groupBy
     */

    // rdd3中的一行，就代表了原来rdd2中的一组数据
    // RDD3{
    //     doit29,Iterable[student,student,student,.....] ==>  ?
    //     doit30,Iterable[student,student,student,.....] ==>  ?
    // }
    val rdd3: RDD[(String, Iterable[Student])] = rdd2.groupBy(stu => stu.term)

    val rdd4: RDD[(String, Double)] = rdd3.map(tp=>{
      val term = tp._1
      val sum = tp._2.map(stu=>stu.score).sum
      (term,sum)
    })

    rdd4.foreach(println)


    /**
     *  KV结构的RDD才能调用groupByKey
     *  groupByKey
     */
    val rdd5: RDD[(String, Double)] = rdd2.map(stu => (stu.term, stu.score))
    val rdd6: RDD[(String, Iterable[Double])] = rdd5.groupByKey()

    rdd6.map(tp=>(tp._1,tp._2.sum))


    sc.stop()

  }

}
