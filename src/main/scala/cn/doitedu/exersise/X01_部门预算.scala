package cn.doitedu.exersise

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

/**
 * 求，预算最低的3个部门的： 部门id，部门预算，部门人数
 *    预算最高的3个部门的： 部门id，部门预算，部门员工总薪资
 * 用sparkSql开发
 */
object X01_部门预算 {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("")
      .master("local")
      .getOrCreate()


    val schema1 = StructType(Seq(
      StructField("departmentid",DataTypes.IntegerType),
      StructField("yusuanAmount",DataTypes.IntegerType)
    ))

    //departmentid,employeeid,salary
    val schema2 = StructType(Seq(
      StructField("departmentid",DataTypes.IntegerType),
      StructField("employeeid",DataTypes.IntegerType),
      StructField("salary",DataTypes.IntegerType)
    ))


    val yuSuan = spark.read.schema(schema1).option("header", "true").csv("data/yusuan/yusuan.txt")
    val department = spark.read.schema(schema2).option("header", "true").csv("data/yusuan/department.txt")

    yuSuan.createTempView("yusuan")
    department.createTempView("department")

    val res = spark.sql(
      """
        |select
        |  o1.departmentid,
        |  o1.yusuanAmount,
        |  o2.cnt
        |
        |from
        |(
        |   select
        |     departmentid,yusuanAmount
        |   from yusuan
        |   order by yusuanAmount
        |   limit 3
        |) o1
        |
        |join
        |
        |(
        |   select
        |     departmentid,
        |     count(1) as cnt
        |   from department
        |   group by departmentid
        |) o2
        |
        |on o1.departmentid=o2.departmentid
        |
        |""".stripMargin)

    res.show(100,false)


    /**
     * 求每个部门薪资最高的3个员工，以及该部门的今年的预算金额
     */
    val res2 = spark.sql(
      """
        |select
        |     o2.departmentid,
        |     o2.employeeid,
        |     o2.salary,
        |     yusuan.yusuanAmount
        |from
        |(
        |
        |   select
        |     departmentid,
        |     employeeid,
        |     salary
        |   from
        |   (
        |      select
        |         departmentid,
        |         employeeid,
        |         salary,
        |         row_number() over(partition by departmentid order by salary desc) as rn
        |      from department
        |   ) o
        |   where rn<=3
        |) o2
        |join
        |yusuan
        |on o2.departmentid=yusuan.departmentid
        |
        |""".stripMargin)
    res2.show(100,false)

    spark.close()



  }

}
