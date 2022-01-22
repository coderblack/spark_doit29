package cn.doitedu.course

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

// 1,zs,doit29,80
case class Soldier(id: Int, name: String, term: String, score: Int)

object C22_缓存RDD {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf(false)
    conf.setMaster("local")
    conf.setAppName("")

    val sc = new SparkContext(conf)
    // 如果代码中调用了checkpoint，那么就需要对sc设置checkpoint的目录
    sc.setCheckpointDir("hdfs://doit01:8020/spark-check-01/")

    val rdd1: RDD[String] = sc.textFile("data/wordcount/input/wc3.txt")

    val rdd2: RDD[Soldier] = rdd1.flatMap(line => {
      val arr = line.split("\\|")
      arr.map(line => {
        val splits = line.split(",")
        Soldier(splits(0).toInt, splits(1), splits(2), splits(3).toInt)
      })
    })

    // 后续将会在rdd2的基础上做各种各样的运算job
    // 那么，就可以把rdd2缓存起来（把rdd2的计算结果落地--内存、task的本地磁盘上）
    // 后续的那些job，就不需要再重走  file->rdd1->rdd2 的过程了
    // rdd2.cache()  // 必须要有一个job触发，走了 file->rdd1->rdd2的流程，才会真正缓存下rdd2的数据

    rdd2.persist(StorageLevel.MEMORY_ONLY)  // cache() // 就是把数据缓存在内存中

    rdd2.checkpoint()  // checkpoint会额外生成一个job，来将数据输出到hdfs

    // 统计每个班的最高分
    val res1 = rdd2
      .groupBy(soldier => soldier.term)
      .map(tp => {
        (tp._1,tp._2.map(_.score).max)
      })

    // 统计每个班的总分
    val res2 = rdd2
      .groupBy(soldier=>soldier.term)
      .map(tp=>{
        (tp._1,tp._2.map(_.score).sum)
      })

    // 统计每个班的人数
    val res3 = rdd2
      .groupBy(soldier=>soldier.term)
      .map(tp=>{
        (tp._1,tp._2.size)
      })


    res1.count()
    res2.count()
    res3.count()


    Thread.sleep(Long.MaxValue)
    sc.stop()


  }

}
