package cn.doitedu.exersise

import ch.hsr.geohash.GeoHash
import org.apache.spark.sql.SparkSession

import java.util.Properties

/**
 * 1. 将app的用户行为日志数据加载到一个hive的表中（要求按天分区）
 * 2. 将mysql中的一个gps坐标点参考表，把所有的3、4级行政单位参考点，加工成： 经度,纬度,省,市,区
 * 3. 将加工好的参考点表再次加工：把经度、维度转成geoHash编码，并将结果写入hive表
 * 4. 用spark对hive中的app行为日志表，进行etl（将每条数据中的经纬度坐标，转成：省市区 地理名称）并存入一张新的hive表
 *
 */
object X04_综合练习_地理位置映射 {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("")
      .config("spark.sql.shuffle.partitions","1")
      .master("local")
      .enableHiveSupport()
      .getOrCreate()


    // 定义一个将gps坐标转成geohash码的函数
    val gps2GeoHash = (lat:Double,lng:Double)=>GeoHash.geoHashStringWithCharacterPrecision(lat,lng,5)
    spark.udf.register("gps2geo",gps2GeoHash)


    // gpsReference2Geohash(spark)
    integrateArea(spark)

    spark.close()

  }

  def gpsReference2Geohash(spark:SparkSession): Unit ={
    val props = new Properties()
    props.setProperty("user","root")
    props.setProperty("password","123456")
    val df = spark.read.jdbc("jdbc:mysql://localhost:3306/abc", "t_md_areas", props)
    df.createTempView("df")

    val res = spark.sql(
      """
        |
        |select
        |   geohash,
        |   province,
        |   city,
        |   region
        |from (
        |    select
        |        gps2geo(l4.bd09_lat,l4.bd09_lng) as geohash,
        |        l1.areaname as province,
        |        l2.areaname as city,
        |        l3.areaname as region
        |    from df l4 join df l3 on l4.parentid=l3.id and l4.level=4
        |               join df l2 on l3.parentid=l2.id
        |               join df l1 on l2.parentid=l1.id
        |
        |    UNION ALL
        |
        |    select
        |        gps2geo(l3.bd09_lat,l3.bd09_lng) as geohash,
        |        l1.areaname as province,
        |        l2.areaname as city,
        |        l3.areaname as region
        |    from df l3 join df l2 on l3.parentid=l2.id and l3.level=3
        |               join df l1 on l2.parentid=l1.id
        |) o
        |group by geohash,province,city,region
        |
        |""".stripMargin)

    res.write.saveAsTable("doit29.ref_geo")

  }

  def integrateArea(spark:SparkSession): Unit ={

    spark.sql(
      """
        | -- join取数
        |select
        |  account        ,
        |  appid          ,
        |  appversion     ,
        |  carrier        ,
        |  deviceid       ,
        |  devicetype     ,
        |  eventid        ,
        |  ip             ,
        |  latitude       ,
        |  longitude      ,
        |  nettype        ,
        |  osname         ,
        |  osversion      ,
        |  properties     ,
        |  releasechannel ,
        |  resolution     ,
        |  sessionid      ,
        |  `timestamp`    ,
        |  province       ,
        |  city           ,
        |  region
        |
        |from
        |(
        |  -- 读日志表
        |  select
        |     *,gps2geo(latitude,longitude) as geohash
        |  from doit29.app_log where dt='2021-12-10'
        |) o1
        |
        |join
        |
        |(
        |  -- 读参考地理位置表
        |  select
        |    geohash,province,city,region
        |  from doit29.ref_geo
        |) o2
        |on o1.geohash = o2.geohash
        |
        |""".stripMargin).show(20,false)


  }


}
