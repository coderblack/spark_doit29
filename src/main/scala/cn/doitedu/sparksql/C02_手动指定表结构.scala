package cn.doitedu.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

object C02_手动指定表结构 {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("手动指定表结构")
      .master("local[*]")
      .getOrCreate()

    // 创建一个schema（表结构：字段名 字段类型）
    // 1,zs,doit29,80
    val schema = StructType(Seq(
      StructField("id",DataTypes.IntegerType),
      StructField("name",DataTypes.StringType),
      StructField("term",DataTypes.StringType),
      StructField("score",DataTypes.DoubleType)
    ))

    // 加载csv结构的数据文件成为一个dataset
    val ds = spark.read.schema(schema).csv("data/wordcount/input/wc2.txt")

    ds.printSchema()
    ds.show()

    spark.close()
  }

}
