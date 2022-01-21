package cn.doitedu.wc

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()


    conf.setAppName("单词统计")

    val sc = new SparkContext(conf)

    // 这里写的路径，凭什么认为是 hdfs文件系统的？
    // 看环境中是否有参数:  fs.defaultFS = hdfs://doit01:8020/
    sc.textFile(args(0))
      .flatMap(_.split("\\s+"))
      .map((_,1))
      .reduceByKey(_+_)
      .saveAsTextFile(args(1))


    Thread.sleep(Long.MaxValue)


    sc.stop()
  }
}
