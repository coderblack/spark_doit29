package cn.doitedu.sparksql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object X02_随堂练习_2自定义udf函数 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)

    val spark = SparkSession.builder()
      .appName("")
      .master("local")
      .getOrCreate()

    import spark.implicits._

     // (1,"aa",175.0,78,67.7,90.9)
     // (2,"bb",178.0,18,87.7,62.9)
     // (3,"cc",120.0,88,72.7,165.9)
     // (4,"dd",180.0,78,73.0,61.9)
     // (5,"ee",162.0,98,61.0,190.2)
     // (6,"ff",162.0,68,72.1,98.1)

    // 写一个sql，查询如下逻辑：  id,name,特征平均值







  }

}
