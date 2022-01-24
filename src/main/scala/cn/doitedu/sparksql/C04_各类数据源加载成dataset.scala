package cn.doitedu.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

object C04_各类数据源加载成dataset {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("")
      .master("local")
      .getOrCreate()

    // loadCsv(spark)
    // loadJson(spark)
    loadJson2(spark)

    spark.close()
  }


  def loadCsv(spark: SparkSession): Unit = {
    val df = spark.read.option("header", "true").csv("data/sql/wc2.txt")
    val df2 = df.toDF("stu_id", "stu_name", "term", "score") // 为df的各个字段快速重命名
    df2.printSchema()
    df2.show()
  }

  def loadJson(spark: SparkSession): Unit = {

    val schema = StructType(Seq(
      StructField("id", DataTypes.IntegerType),
      StructField("name", DataTypes.StringType),
      StructField("age", DataTypes.DoubleType),
      StructField("gender", DataTypes.StringType)
    ))

    val df = spark.read.schema(schema).json("data/sql/stu.txt") // 如果不手动传schema，自动推断schema默认是true，会额外触发job
    df.printSchema()
    df.show()
  }


  // json中有嵌套的数组
  def loadJson2(spark: SparkSession): Unit = {

    // {"id":1,"name":"aa","scores":[95,80,86,87],"age":18,"gender":"male"}
    val schema = StructType(Seq(
      StructField("id", DataTypes.IntegerType),
      StructField("name", DataTypes.StringType),
      StructField("school",
        DataTypes.createStructType(Array(
          StructField("name",DataTypes.StringType),
          StructField("graduate",DataTypes.StringType),
          StructField("rank",DataTypes.IntegerType)))
      ),
      StructField("info", DataTypes.createMapType(DataTypes.StringType,DataTypes.StringType)),   // info字段，因为每一行中的内部属性不同，所以用HashMap类型比较合适
      StructField("scores", DataTypes.createArrayType(DataTypes.IntegerType)), // 数组类型
      StructField("age", DataTypes.DoubleType),
      StructField("gender", DataTypes.StringType)
    ))

    // val df = spark.read.json("data/sql/stu2.txt")   // 自动推断的话，info字段会被推断成 struct类型
    val df = spark.read.schema(schema).json("data/sql/stu2.txt")
    df.printSchema()
    df.show(false)


    // TODO
    df.createTempView("t")
    spark.sql("""""")

  }

}
