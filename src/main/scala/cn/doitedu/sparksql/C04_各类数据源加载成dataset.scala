package cn.doitedu.sparksql

import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}
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
    // loadJDBC(spark)

    df写出成各种数据源(spark)
    // loadParquet(spark)
    // loadOrc(spark)

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
      // 定义基本类型字段
      StructField("id", DataTypes.IntegerType),
      StructField("name", DataTypes.StringType),
      // 定义struct类型字段
      StructField("school",
        DataTypes.createStructType(Array(
          StructField("name", DataTypes.StringType),
          StructField("graduate", DataTypes.StringType),
          StructField("rank", DataTypes.IntegerType)))
      ),
      // 定义Map类型字段
      StructField("info", DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType)), // info字段，因为每一行中的内部属性不同，所以用HashMap类型比较合适
      // 定义Array类型字段
      StructField("scores", DataTypes.createArrayType(DataTypes.IntegerType)), // 数组类型
      StructField("age", DataTypes.DoubleType),
      StructField("gender", DataTypes.StringType)
    ))

    // val df = spark.read.json("data/sql/stu2.json")   // 自动推断的话，info字段会被推断成 struct类型
    val df = spark.read.schema(schema).json("data/sql/stu2.json")
    df.printSchema()
    df.show(false)

    spark.read.parquet()


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

  // 读parquet数据源
  // parquet数据源不用手动传schema，因为parquet是自我描述型的列式存储文件格式（文件内部就带有表结构信息）
  def loadParquet(spark:SparkSession):Unit = {
    val df = spark.read.parquet("data/sql/out-parquet")
    df.printSchema()
    df.show(false)
  }

  // 读orc数据源
  // orc文件源不用手动传schema，因为 orc 是自我描述型的列式存储文件格式（文件内部就带有表结构信息）
  def loadOrc(spark:SparkSession):Unit = {
    val df = spark.read.orc("data/sql/out-orc")
    df.printSchema()
    df.show(false)
  }


  def df写出成各种数据源(spark:SparkSession):Unit = {
    /**
     * 先从文件中，产生一个 dataframe
     */
    val schema = StructType(Seq(
      StructField("id",DataTypes.IntegerType),
      StructField("name",DataTypes.StringType),
      StructField("age",DataTypes.IntegerType),
      StructField("gender",DataTypes.StringType),
      StructField("info",DataTypes.createMapType(DataTypes.StringType,DataTypes.StringType)),
      StructField("scores",DataTypes.createArrayType(DataTypes.IntegerType)),
      StructField("school",DataTypes.createStructType(Array(StructField("name",DataTypes.StringType),StructField("graduate",DataTypes.StringType),StructField("rank",DataTypes.IntegerType))))
    ))
    val df: DataFrame = spark.read.schema(schema).json("data/sql/stu2.json")

    /**
     * 一、df 保存为各类格式的文件
     */
    df.write.parquet("data/sql/out-parquet/")   // 1.将df写成一个parquet文件
    df.write.orc("data/sql/out-orc/")   // 2.将df写成一个orc文件
    df.write.json("data/sql/out-json/")  // 3.将df写成一个json文件
    // df.write.csv("data/sql/out-csv/")    // 简单结构的dataframe才能写成csv文件 ,不支持array、map、struct复合类型
    val df2 = df.drop("info").drop("scores").drop("school") // drop方法:丢弃列
    df2.write.csv("data/sql/out-csv/")   // 4.将df写成一个 csv 文件

    /**
     * 二、df 保存为 mysql 的表
     * 如果表不存在，会自动建表
     * 注意： jdbc中不存在array、map、struct类型
     */
    val props = new Properties()
    props.setProperty("user","root")
    props.setProperty("password","123456")

    val df3 = df.drop("scores", "school", "info")  // 因为jdbc中不存在array、map、struct类型

    // 输出数据时：可以选择保存模式（SaveMode）
    // SaveMode有多种取值:
    //    overWrite ==> 覆盖写入；  append ==> 追加写入 ;
    //    Ignore ==> 如果目标表已存在则啥也不做  ErrorIfExists ==> 如果目标表已存在，就抛个异常给你
    df3.write.mode(SaveMode.Overwrite).jdbc("jdbc:mysql://localhost:3306/abc","t_x",props)

  }
}
