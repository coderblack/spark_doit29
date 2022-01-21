package cn.doitedu.course

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object C19_SHUFFLE算子不一定产生Shuffle {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[2]")
    conf.setAppName("")
    conf.set("spark.default.parallelism", "2")
    val sc = new SparkContext(conf)


    val rdd1: RDD[(Int, String, String, Int)] = sc.makeRDD(Seq(
      (1, "a", "male", 18),
      (4, "d", "female", 38),
      (2, "b", "male", 28),
      (5, "e", "male", 22),
      (3, "c", "female", 18),
      (6, "f", "female", 22),
      (7, "g", "male", 28),
      (8, "h", "female", 34)
    ), 4)


    /**
     * shuffle算子不一定生成shuffle
     */
    val x2 = rdd1.map(tp => (tp._3, tp))

    // 这里一定产生shuffle，因为 x2没有分区器, 而x3的分区器是  HashPartitioner(2)
    val x3 = x2.partitionBy(new HashPartitioner(2))

    /**
     * 1. 注意与 2 的区别
     */
    // map 算子会丢失原来的rdd的分区器
    val x3_1 = x3.map(tp => tp)  // map后得到的x3_1的分区器：  None
    println("map后得到的x3_1的分区器：  " + x3_1.partitioner)

    // 这个groupByKey会产生shuffle，因为 x3_1已经丢失了分区器
    val x4_1 = x3_1.groupByKey(2)
    x4_1.count()

    /**
     * 2. 注意与 1 的区别
     */
    // mapPartitions 可以选择保留分区器
    val x3_2 = x3.mapPartitions(iter => iter, true) // mapPartitions后得到的x3_2的分区器：  Some(org.apache.spark.HashPartitioner@2)
    println("mapPartitions后得到的x3_2的分区器：  " + x3_2.partitioner)

    // 这个groupByKey不会产生shuffle，因为 x3_2有分区器且与x4_2的分区器相同
    val x4_2 = x3_2.groupByKey(2)
    x4_2.count()


    Thread.sleep(Long.MaxValue)
    sc.stop()
  }
}
