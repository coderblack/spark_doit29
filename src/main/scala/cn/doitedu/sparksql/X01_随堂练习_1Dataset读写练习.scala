package cn.doitedu.sparksql

import org.apache.spark.sql.SparkSession

import java.util.Properties

object X01_随堂练习_1Dataset读写练习 {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("")
      .master("local")
      .config("spark.sql.shuffle.partitions",1)  // sparkSql中的shuffle默认并行度参数
      .getOrCreate()

    // 把 mysql中的一个表映射成 sparkSql的一个 dataframe
    val props = new Properties()
    props.setProperty("user","root")
    props.setProperty("password","123456")

    val df = spark.read.jdbc("jdbc:mysql://localhost:3306/abc", "t_md_areas", props)


    // 然后在sparkSql中写sql来进行查询(查询到所有的3级、4级行政单位地点）得到如下结果：
    //      江苏省,盐城市,大丰区,120.58506449026754,33.26590852607762
    df.createTempView("df")
    val res = spark.sql(
      """
        |
        |select
        |    level_1.areaname as province,
        |    level_2.areaname as city,
        |    level_3.areaname as region,
        |    level_4.bd09_lng as lng,
        |    level_4.bd09_lat as lat
        |from df level_4  join  df level_3   on  level_4.parentid=level_3.id  and level_4.level=4
        |                 join  df level_2   on  level_3.parentid=level_2.id
        |                 join  df level_1   on  level_2.parentid=level_1.id
        |
        |union all
        |
        |select
        |    level_1.areaname as province,
        |    level_2.areaname as city,
        |    level_3.areaname as region,
        |    level_3.bd09_lng as lng,
        |    level_3.bd09_lat as lat
        |from df level_3  join  df level_2   on  level_3.parentid=level_2.id  and level_3.level=3
        |                 join  df level_1   on  level_2.parentid=level_1.id
        |
        |
        |""".stripMargin)


    // 然后将结果写成一个parquet文件，并也写入mysql
    res.write.parquet("data/x01out/")
    res.write.jdbc("jdbc:mysql://localhost:3306/abc","x01_out",props)


    spark.close()

  }
}
