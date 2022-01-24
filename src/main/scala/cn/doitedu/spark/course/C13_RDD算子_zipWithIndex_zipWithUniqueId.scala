package cn.doitedu.spark.course

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object C13_RDD算子_zipWithIndex_zipWithUniqueId {

  def main(args: Array[String]): Unit = {


    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf = new SparkConf
    conf.setAppName("")
    conf.setMaster("local")

    val sc = new SparkContext(conf)


    val rdd1: RDD[String] = sc.makeRDD(Seq("a", "b", "c", "a", "d", "e"),3)

    // zipWithIndex，就是把rdd中的元素都拉上元素的全局连续递增的索引号
    // 分区中第n元素的uniqueId = 分区号 + n*分区数
    val rdd2: RDD[(String, Long)] = rdd1.zipWithIndex()  // 内部会触发一个job，来获知rdd1的每个分区中的元素个数（最后一个分区除外）
    rdd2.foreach(println)
    /**
     * (a,0)
     * (b,1)
     * (c,2)
     * (a,3)
     * (d,4)
     */
      println("------------------涛哥又来了-------------------------")

    // zipWitchUniqueId ，把rdd中的元素都拉上一个唯一id  （生成的id之间是有间隙的）
    val rdd3: RDD[(String, Long)] = rdd1.zipWithUniqueId()
    rdd3.foreach(println)
    /**
     * (a,0)
       (b,3)   分区0
       ---------
       (c,1)   分区1
       (a,4)
       (d,2)
       (e,5)
     */

    sc.stop()


  }

}
