package cn.doitedu.spark.course

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object C10_RDD算子_aggregateByKey {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf = new SparkConf
    conf.setAppName("")
    conf.setMaster("local")

    val sc = new SparkContext(conf)

    // 做测试：直接将内存集合转成RDD
    val lst = List(("a",1),("a",2),("b",3),("b",1),("b",2))
    val rdd1: RDD[(String, Int)] = sc.makeRDD(lst)  // sc.parallelize(lst)


    // 复习reduceByKey
    val res1: RDD[(String, Int)] = rdd1.reduceByKey(_ + _)
    res1.foreach(println)


    println("------------------------------")

    // 拓展aggregateByKey
    // 参数1： 初始值
    // 参数2： 局部聚合函数（累计值，元素值）=>新累计值
    // 参数3： 全局聚合函数（累计值1，累计值2）=> 新累计值
    val res2 = rdd1.aggregateByKey[Int](0)((acc, ele) => acc + ele, (acc1, acc2) => acc1 + acc2)
    res2.foreach(println)


    println("------------------------------")

    // aggregateByKey与reduceByKey最大的一个不同是：
    // aggregateByKey允许把元素A类型，聚合成结果B类型，没有约束
    // reduceByKey约束了：元素类型和聚合的结果类型一致
    val res3: RDD[(String, String)] = rdd1.aggregateByKey[String]("")((acc, ele) => acc + ele, (acc1, acc2) => acc1 + acc2)
    res3.foreach(println)


    //  把上面的数据按相同字母分组聚合成
    //  a,List(1,2)
    //  b,List(3,1,2)
    val res4 = rdd1.aggregateByKey[List[Int]](Nil)((lst:List[Int],ele:Int)=>{ele :: lst},(lst1:List[Int],lst2:List[Int])=>lst1 ::: lst2)
    res4.foreach(println)







    sc.stop()


  }
}
