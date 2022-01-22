package cn.doitedu.wc

import org.apache.spark.{SparkConf, SparkContext}

object MemoryTest {

  def main(args: Array[String]): Unit = {

    // 代码中临时设置日志打印级别
    // Logger.getLogger("org").setLevel(Level.WARN)

    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("wordCount")

    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(Seq(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)).coalesce(1)
    var n = 1
    n=1
    val res = rdd.map(e=>{
      val gn = new Array[Array[Long]](n)
      for(i <- 0 until n){
        val g1 = new Array[Long](1024 * 1024 * 1024 / 16)
        val g11 = g1.zipWithIndex.map(_._2.toLong)
        gn(i) = g11
      }
      e
    })
    res.count()

    Thread.sleep(Long.MaxValue)


  }

}
