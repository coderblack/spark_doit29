package cn.doitedu.sparksql

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

import java.util.Properties

object C04_各类数据源加载成dataset {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("")
      .master("local")
      .getOrCreate()

    // loadCsv(spark)
    // loadJson(spark)
    // loadJson2(spark)
    loadJDBC(spark)


    spark.close()
  }


  // 读csv文件数据源
  def loadCsv(spark: SparkSession): Unit = {
    val df = spark.read.option("header", "true").csv("data/sql/wc2.txt")
    val df2 = df.toDF("stu_id", "stu_name", "term", "score") // 为df的各个字段快速重命名
    df2.printSchema()
    df2.show()
  }

  // 读简单json数据源
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

  // 读复杂嵌套json数据源
  def loadJson2(spark: SparkSession): Unit = {

    // {"id":1,"name":"aa","scores":[95,80,86,87],"age":18,"gender":"male"}
    val schema = StructType(Seq(
      StructField("id", DataTypes.IntegerType),
      StructField("name", DataTypes.StringType),
      StructField("school",
        DataTypes.createStructType(Array(
          StructField("name", DataTypes.StringType),
          StructField("graduate", DataTypes.StringType),
          StructField("rank", DataTypes.IntegerType)))
      ),
      StructField("info", DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType)), // info字段，因为每一行中的内部属性不同，所以用HashMap类型比较合适
      StructField("scores", DataTypes.createArrayType(DataTypes.IntegerType)), // 数组类型
      StructField("age", DataTypes.DoubleType),
      StructField("gender", DataTypes.StringType)
    ))

    // val df = spark.read.json("data/sql/stu2.txt")   // 自动推断的话，info字段会被推断成 struct类型
    val df = spark.read.schema(schema).json("data/sql/stu2.txt")
    df.printSchema()
    df.show(false)


    // TODO  计算每所学校毕业生的平均年龄
    df.createTempView("df")
    val res = spark.sql(
      """
        |
        |select
        |  school.name as school_name,
        |  avg(age) as avg_age
        |from df
        |group by school.name
        |""".stripMargin)

    res.show(100, false)
  }

  // 读jdbc的表数据源
  def loadJDBC(spark: SparkSession): Unit = {
    val props = new Properties()
    props.setProperty("user", "root")
    props.setProperty("password", "123456")
    val df: Dataset[Row] = spark.read.jdbc("jdbc:mysql://localhost:3306/abc", "stu", props)
    df.printSchema()
    df.show(100, false)

    // TODO   计算年龄最大的前4人 --> 全局topN
    //        计算每种性别中的年龄最大前2人 --> 分组topN
    df.createTempView("stu")

    // 计算年龄最大的前4人 --> 全局topN
    val res1 = spark.sql(
      """
        |
        |select
        |  *
        |from stu
        |order by age desc
        |limit 4
        |
        |""".stripMargin)
    res1.show(100, false)

    // 计算每种性别中的年龄最大前2人 --> 分组topN
    val res2 = spark.sql(
      """
        |select
        |   id,name,age,score,gender
        |from
        |  (
        |     select
        |       id,
        |       name,
        |       age,
        |       score,
        |       gender,
        |       row_number() over(partition by gender order by age desc) as rn
        |     from stu
        |  ) o
        |where rn<=2
        |
        |""".stripMargin)
    res2.show(false)

  }

}
