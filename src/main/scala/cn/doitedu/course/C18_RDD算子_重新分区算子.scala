package cn.doitedu.course

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

object C18_RDD算子_重新分区算子 {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setMaster("local[2]")
    conf.setAppName("")
    conf.set("spark.default.parallelism","2")
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

    // coalesce 算子 改变分区数
    val rdd2 = rdd1.coalesce(2) // 分区数变小，可以不需要shuffle
    val rdd3 = rdd1.coalesce(8, true) // 分区数变大，需要将shuffle设置为true

    // repartition 算子 改变分区数
    // 底层调用的就是coalesce
    val rdd4 = rdd1.repartition(2) // 分区数变小，底层写死了shuffle=true
    val rdd5 = rdd1.repartition(8) // 分区数变大，底层写死了shuffle=true


    /**
     * 想对rdd1中的数据 ，按年龄大小分两个区，年龄小的在partition_0, 年龄大的在 partition_1
     */
      // 首先，通过一个job，去获取rdd1中年龄数据的最大值和最小值
    val ages: Array[Int] = rdd1.map(_._4).sample(false, 0.8).collect()
    val lowerBound: Int = ages.min
    val upperBound: Int = ages.max

    // 取最大值和最小值之间的分界点
    val middle = lowerBound +  (upperBound-lowerBound)/2


    // partitionBy  按指定字段分区
    // 自定义了一个  按key的值的范围分区的 分区器
    // key的值<分界点的在一个分区； 其他的在另一个分区
    class MyPartitioner(middle:Int) extends Partitioner{
      override def numPartitions: Int = 2
      override def getPartition(key: Any): Int = {
        if(key.asInstanceOf[Int] < middle ) 0 else 1
      }
    }

    // 然后调用partitionBy来使用我们自定义的分区器
    val rdd7 = rdd1.map(tp => (tp._4, tp)).partitionBy(new MyPartitioner(middle))
    rdd7.saveAsTextFile("data/partition")


    // 从上面的例子，可以去揣测 sortBy算子底层的分区器：RangePartitioner  （跟我们上面例子相似的，值域范围分区器）
    val rdd6 = rdd1.sortBy(tp => tp._4, true, 2)
    rdd6.saveAsTextFile("data/sort/")


    sc.stop()

  }

}
