package cn.doitedu.sparksql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row, SparkSession}

object C13_dataset上的rdd算子 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("")
      .master("local")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    val ds1: Dataset[(Int, String, String, Int)] = spark.createDataset(Seq(
      (1,"zs","male",18),
      (2,"ls","female",28),
      (3,"aa","male",38),
      (4,"aa","female",26)
    ))

    val ds2: Dataset[(Int, String, Int)] = ds1.map(tp=>{
      (tp._1,  tp._3,   tp._4*10+tp._1)
    })


    val ds3: Dataset[(Int, String, String, Int)] = ds1.filter(tp => tp._4 <= 30)


    // 如果是在dataframe上调rdd算子，则传给我们函数的数据是Row类型
    val df = ds1.toDF("id", "name", "gender", "age")

    val ds4: Dataset[(Int, String, Int)] = df.map(row=>{
      // 从row中取数据的方式1：用get方法
      val id = row.getAs[Int]("id")
      val name = row.getAs[String]("name")
      val gender = row.getAs[String]("gender")
      val age = row.getAs[Int]("age")
      (id,gender,age*10+id)
    })

    // 从row中取数据的方式2：用模式匹配
    val ds5: Dataset[(Int, String, Int)] = df.map({
      case Row(id:Int,name:String,gender:String,age:Int) => (id,gender,age*10+id)
    })



    // 还有一种办法调rdd算子
    val rdd2: RDD[(String, String, Int)] = ds1.rdd.map(tp => (tp._3, tp._2, tp._1))


  }
}
