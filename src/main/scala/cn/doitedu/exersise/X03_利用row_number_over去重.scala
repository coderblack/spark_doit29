package cn.doitedu.exersise

import org.apache.spark.sql.SparkSession

object X03_利用row_number_over去重 {
  def main(args: Array[String]): Unit = {

    /**
     * 对如下数据去重，去重的要求是： id相同的保留任意一行
     * 1,zs,18
       1,ls,28
       2,ww,38
       2,zl,44
       3,qt,55
       4,aa,23
     */

    val spark = SparkSession.builder()
      .appName("")
      .master("local")
      .getOrCreate()


    spark.read.csv("data/distinct").toDF("id","name","age").createTempView("df")


    spark.sql(
      """
        |select
        |  id,name,age
        |from
        |(
        |select
        |   id,
        |   name,
        |   age,
        |   row_number() over(partition by id order by age desc) as rn
        |from df
        |) o
        |where rn=1
        |
        |""".stripMargin).show(100,false)

    spark.close()
  }

}
