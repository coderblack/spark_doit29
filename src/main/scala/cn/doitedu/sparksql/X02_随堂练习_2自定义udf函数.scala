package cn.doitedu.sparksql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{udaf, udf}

object X02_随堂练习_2自定义udf函数 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)

    val spark = SparkSession.builder()
      .appName("")
      .master("local")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    val rdd: RDD[String] = spark.sparkContext.textFile("data/sql/features.txt")
    val rdd2 = rdd.map(line=>{
      val splits = line.split(",")
      val strings = splits(2).split(":")
      val features: Array[Double] = strings.flatMap(s => s.split("_")).map(_.toDouble)
      (splits(0).toInt,splits(1),features)
    })

    // 把rdd2转成dataset
    val df = rdd2.toDF("id", "name", "features")

    // 注册一张表
    df.createTempView("df")

    // 写一个sql，查询如下逻辑：  id,name,特征平均值

    val myAvg = (arr:Array[Double])=> arr.sum/arr.length
    spark.udf.register("my_avg",myAvg)

    spark.sql(
      """
        |
        |select
        |  id,
        |  name,
        |  my_avg(features) as avg_feature
        |from df
        |
        |""".stripMargin).show()

    spark.close()

  }

}
