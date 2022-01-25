package cn.doitedu.sparksql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object C09_自定义UDF函数 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)

    val spark = SparkSession.builder()
      .appName("")
      .master("local")
      .getOrCreate()

    import spark.implicits._

    val ds_sample = spark.createDataset(Seq(
      (1,"aa",175.0,67.7,68.9,"male"),
      (2,"bb",178.0,77.7,62.9,"male"),
      (3,"cc",185.0,72.7,64.9,"male"),
      (4,"dd",180.0,73.0,61.9,"male"),
      (5,"ee",162.0,61.0,98.2,"female"),
      (6,"ff",162.0,62.1,98.1,"female"),
      (7,"gg",165.0,60.2,98.3,"female"),
      (8,"hh",170.0,62.7,97.9,"female")
    )).toDF("id","name","height","weight","face","gender")

    val ds_test = spark.createDataset(Seq(
      (10,"xx",175.3,78.7,68.9),
      (20,"yy",176.2,78.2,66.9),
      (30,"oo",162.0,62.7,93.6),
      (40,"uu",161.5,63.2,92.4)
    )).toDF("id","name","height","weight","face")

    ds_sample.createTempView("sample")
    ds_test.createTempView("test")


    // 先定义一个普通的scala函数
    val euDist = (arr1:Array[Double],arr2:Array[Double])=>{
      Math.pow(arr1.zip(arr2).map(tp=>Math.pow(tp._1-tp._2,2.0)).sum,0.5)
    }
    // 然后注册成sql函数
    spark.udf.register("dist",euDist)


    val tmp = spark.sql(
      """
        |
        |select
        | test.id as test_id,
        | test.name as test_name,
        | dist(array(test.height,test.weight,test.face),array(sample.height,sample.weight,sample.face)) as difference,
        | sample.gender as sample_gender
        |
        |from test cross join sample
        |sort by test_id
        |
        |""".stripMargin)
    tmp.createTempView("tmp")


    spark.sql(
      """
        |select
        |  id,
        |  name,
        |  gender
        |
        |from
        |(
        |select
        |   test_id as id,
        |   test_name as name,
        |   sample_gender as gender,
        |   row_number() over(partition by test_id order by difference) as rn
        |from tmp ) o
        |where rn<=3
        |
        |""".stripMargin).createTempView("tmp2")
    /**
     * +---+----+------+
       | id|name|gender|
       +---+----+------+
       | 20|  yy|  male|
       | 20|  yy|female|
       | 20|  yy|  male|
       | 40|  uu|female|
       | 40|  uu|female|
       | 40|  uu|female|
     */


    spark.sql(
      """
        |select
        | id,name,gender
        |from tmp2
        |group by id,name,gender
        |having count(1)>1
        |
        |""".stripMargin).show(100,false)



    spark.close()


  }

}
