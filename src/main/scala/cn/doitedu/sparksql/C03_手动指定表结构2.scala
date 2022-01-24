package cn.doitedu.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

object C03_手动指定表结构2 {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("")
      .master("local")
      .getOrCreate()

    val schema = StructType(Seq(
      StructField("id", DataTypes.IntegerType),
      StructField("stu_name", DataTypes.StringType),
      StructField("class", DataTypes.StringType),
      StructField("power", DataTypes.IntegerType)
    ))

    //val df = spark.read.schema(schema).option("header","true").csv("data/sql/wc2.txt")
    val df = spark.read
      .option("header", "true")   //  设置，文件有表头
      .schema(schema)     // 手动传入schema（最终的表结构一定是以手动传入的为准！）
      .option("inferSchema", "true")   // 自动推断schema（生产代码中不要用，因为它会额外触发一个job
      .csv("data/sql/wc2.txt")

    df.printSchema()
    df.show()

    spark.close()


  }

}
