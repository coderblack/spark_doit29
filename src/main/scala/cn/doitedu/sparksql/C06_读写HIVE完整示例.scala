package cn.doitedu.sparksql

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object C06_读写HIVE完整示例 {

  def main(args: Array[String]): Unit = {

    /**
     * 示例需求：
     *   1. 从hive中读取doit29.shop_sale表
     *   2. 统计： 店铺，月份，月销售总额，累计到该月的累计总额
     *   3. 将统计结果写入hive存储
     */

    val spark = SparkSession.builder()
      .appName("")
      .config("hive.metastore.uris","thrift://doit01:9083")
      .master("local")
      .enableHiveSupport()
      .getOrCreate()

    // 读hive表
    // val df = spark.read.table("doit29.shop_sale")  // 读法 1
    // df.createTempView("df")


    /**
     * +-------+-------+------------+
       |shop_id|month  |amount_month|
       +-------+-------+------------+
       |1      |2021-01|100         |      100
       |1      |2021-02|30          |      130
       |1      |2021-03|30          |      160
       |1      |2021-04|20          |
       |1      |2021-05|70          |
       |2      |2021-01|30          |
       |2      |2021-02|110         |
       |2      |2021-03|20          |
       |2      |2021-04|90          |
       |2      |2021-05|20          |
       +-------+-------+------------+
     */
    val res: DataFrame = spark.sql(
      """
        |-- insert into table doit29.shop_accu_sale  -- 可以通过insert语句直接将计算结果存到hive的表（要求表已存在）
        |select
        |    shop_id,
        |    month,
        |    amount_month,
        |    sum(amount_month) over(partition by shop_id order by month rows between unbounded preceding and current row)  as amount_accu
        |from (
        |  select
        |    shop_id,
        |    month,
        |    sum(amount) as amount_month
        |  from doit29.shop_sale
        |  group by shop_id,month
        |) o
        |
        |""".stripMargin)


    // 将df写入hive
    res.write.mode(SaveMode.Append).saveAsTable("doit29.shop_accu_sale")

    spark.close()


  }

}
