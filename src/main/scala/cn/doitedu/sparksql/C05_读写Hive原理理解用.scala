package cn.doitedu.sparksql

import org.apache.spark.sql.{DataFrame, SparkSession}

object C05_读写Hive原理理解用 {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .enableHiveSupport()  // 要读写hive，则需要开启hive支持
      .config("hive.metastore.uris","thrift://doit01:9083")  // 加上这个参数，则spark中内置的hive访问的是 指定地址的元数据服务
      .appName("读写hive演示")
      .master("local")
      .getOrCreate()


    // 读 hive
    val res: DataFrame = spark.sql(
      """
        |show tables
        |""".stripMargin)
    res.show()

    val df: DataFrame = spark.read.table("x02_stu") // table 就是读hive的表
    df.show(100, false)


    /*val df = spark.read.json("data/sql/stu.txt")
    df.write.saveAsTable("x02_stu")  // 将df存储为一个hive的表*/




    spark.close()
  }

}
