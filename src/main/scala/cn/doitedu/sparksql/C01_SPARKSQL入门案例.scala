package cn.doitedu.sparksql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}


object C01_SPARKSQL入门案例 {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.WARN)

    // 编程入口： sparkCore=> sparkContext
    //          sparkSql => sparkSession

    val sparkSession = SparkSession.builder()
      .master("local")
      .appName("sparkSql入门体验")
      .config("spark.default.parallelism", 5)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()


    // 创建dataset
    val ds: Dataset[String] = sparkSession.read.textFile("data/wordcount/input/wc2.txt")
    val rdd: RDD[String] = ds.rdd   // dataset中封装了数据的RDD
    ds.printSchema()   // dataset中还封装了数据映射成表的元信息
    ds.show()

    println("--------------要放假了--------------")

    // type DataFrame = Dataset[Row]
    val df: DataFrame = sparkSession.read.csv("data/wordcount/input/wc2.txt")
    val rdd1: RDD[Row] = df.rdd  // dataframe 也是封装了数据的 RDD[Row]
    df.printSchema()   // dataframe 还封装了数据映射成表的元信息
    df.show()
    /**
     * +---+---+------+---+
       |_c0|_c1|   _c2|_c3|
       +---+---+------+---+
       |  1| zs|doit29| 80|
       |  2| ls|doit29| 90|
       |  3| ww|doit30| 70|
       |  4| zl|doit30| 95|
       +---+---+------+---+
     */

    println("--------------要放假了，也不要慌--------------")

    // 将dataset（dataframe） 注册一个表名
    df.createTempView("t_stu")

    // 接下来你就可以直接用sql对这个数据进行运算
    val res: DataFrame = sparkSession.sql(
      """
        |select
        |   _c2 as term,
        |   avg(_c3) as avg_score,
        |   max(_c3) as max_score,
        |   sum(_c3) as sum_score,
        |   min(_c3) as min_score
        |from t_stu
        |group by _c2
        |""".stripMargin)

    res.show()






    sparkSession.close()


  }

}
