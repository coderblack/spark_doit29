package cn.doitedu.exersise

import org.apache.spark.sql.SparkSession

/**
 * 针对登录登出记录，求最大在线人数
 *
 * hive的优化（sparkSql的优化）
 *    1.从业务设计去优化  （大乘）
 *    2.从计算逻辑去优化（sql）  （中乘）
 *    3.参数优化（小乘）
 *    4.数据倾斜的优化（groupby两阶段聚合，join-map端join-采样倾斜key做膨胀）
 *
 */
object X02_最大在线人数 {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("")
      .master("local")
      .config("spark.sql.shuffle.partitions","1")  // spark.default.parallelism
      .getOrCreate()


    // uid,login_time,logout_time
    val df = spark.read.option("header", "true").option("inferSchema", "true").csv("data/maxonline/")
    df.createTempView("df")

    val range2Array  = (s:Int, e:Int)=>{
      s.until(e).toArray
    }
    spark.udf.register("range2arr",range2Array)


    /**
     * 先把每个用户的在线时间区间炸裂成多条
     * u1,1
       u1,2
       u2,1
       u2,2
       u2,3
       u3,2
       u4,3
       u5,2
       u5,3
       u5,4
       u6,3
       u6,4
       u6,5
     然后再统计每一个时间点上的人数
     弊端：数据量急剧膨胀，效率极低！
     */

    spark.sql(
      """
        |select
        |  max(cnt) as max_online
        |from (
        |    select
        |      online_time,count(1) as cnt
        |    from
        |    (
        |       select
        |         uid,
        |         online_time
        |       from df lateral view explode(range2arr(login_time,logout_time)) o as online_time
        |    ) o
        |    group by online_time
        |) o
        |
        |
        |""".stripMargin).show(100,false)


    /**
     * 先把每个用户的登入登出记录变成2条记录，并且打上登入，还是登出的区分标记（1，或-1）
     * u1,1,1
       u1,3,-1
       u2,1,1
       u2,4,-1
       u3,2,1
       u3,3,-1
       u4,3,1
       u4,4,-1
       u5,2,1
       u5,5,-1
       u6,3,1
       u6,6,-1

       -- 时间排序 ，然后sum over 标记字段
       u1,1,1   ==> 2
       u2,1,1   ==> 2
       u3,2,1   ==> 4
       u5,2,1   ==> 4
       u1,3,-1  ==> 4
       u3,3,-1  ==> 4
       u4,3,1   ==> 4
       u6,3,1   ==> 4
       u2,4,-1  ==> 2
       u4,4,-1  ==> 2
       u5,5,-1  ==> 1
       u6,6,-1  ==> 0

       -- sum over时有一个小细节，观察下面的特殊情况，不能用最前加到当前行！
       sum(flag) over(order by time)
       u3,1,1    ==> 1
       u1,2,1    ==> 0
       u2,2,-1   ==> 0
       u3,2,-1   ==> 0
     */

    spark.sql(
      """
        |select
        |  max(cnt) as max_online
        |from
        |(
        |    select
        |      uid,
        |      time,
        |      sum(flag) over(order by time) as cnt
        |    from (
        |        select
        |          uid,
        |          login_time as time,
        |          1 as flag
        |        from df
        |        union all
        |        select
        |          uid,
        |          logout_time as time,
        |          -1 as flag
        |        from df
        |    ) o
        |) o
        |
        |
        |""".stripMargin).show(100,false)




    spark.close()


  }

}
