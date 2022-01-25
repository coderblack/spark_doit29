package cn.doitedu.sparksql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, Row, SparkSession}

object C08_Dataset转Rdd {
  def main(args: Array[String]): Unit = {


    val spark = SparkSession.builder()
      .appName("")
      .master("local")
      .getOrCreate()

    import spark.implicits._

    val ds: Dataset[DoitTeacher] = spark.createDataset(Seq(DoitTeacher(1, "涛哥", 999), DoitTeacher(2, "娜姐", 111)))
    // ds 转 rdd
    val rdd1: RDD[DoitTeacher] = ds.rdd

    // ds 转 df
    val df: Dataset[Row] = ds.toDF()
    // df 转  rdd
    val rdd2: RDD[Row] = df.rdd

    //  df 转回ds，需要把通用的类型Row，转成自定义类型DoitTeacher
    // 需要自定义类型的对应Encoder
    val ds2: Dataset[DoitTeacher] = df.as[DoitTeacher]/*(Encoders.product)*/
    val ds3: Dataset[(Int, String, Int)] = df.as[(Int, String, Int)]/*(Encoders.product)*/


  }

}
