package cn.doitedu.sparksql

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Column, SparkSession}

/**
 * tableApi  就是把sql中的各种关键字，函数，变成 调方法来表达！
 * 这种风格的api，又称之为 “DSL风格API” （DSL:特定领域语言）
 */
object C12_Dataset上的TableApi {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("")
      .master("local")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    // 读hive中的doit29.shop_sale
    val df = spark.read.table("doit29.shop_sale")

    // 查询 店铺id,月份
    df.select("shop_id", "month") /*.show()*/
    // select方法中，认为传入的字符串都是列名
    // 查询表， 输出: id,mth,amount+10
    df.selectExpr("shop_id as id", "month as mth", "amount+10") /*.show()*/
    // selectExpr方法中，认为传入的字符串都是表达式

    // 查询  店铺id，月份，月金额 （过滤掉金额<20的行）  // select shop_id,amount from df where amount>=20
    df.where("amount>=20").select("shop_id", "amount") /*.show()*/

    // 查询  店铺，月份，月总额
    df.groupBy("shop_id", "month")
      .agg(("amount", "sum"), ("amount", "max")) // avg, max, min, sum, count.
      // .select("shop_id","month","sum(amount)","max(amount)")  // select方法中，认为传入的字符窜都是列名
      // .selectExpr("shop_id","month","sum(amount) as amount","max(amount) as max_amount") // selectExpr方法中，认为传入的字符串都是表达式
      .toDF("shop_id", "month", "sum_amount", "max_amount")
    /*.show()*/

    // 排序 sort
    df.sort("shop_id", "amount") /*.show()*/

    // 传Column对象作为参数，功能更强大上
    // val column: Column = df("shop_id")
    df.sort(df("shop_id") asc, df("amount") desc) /*.show()*/
    df.sort($"shop_id" asc, $"amount" desc) /*.show()*/
    df.sort('shop_id asc, 'amount desc) /*.show()*/

    import org.apache.spark.sql.functions._
    df.sort(col("shop_id"), col("amount") desc)

    // 窗口函数  :  求 每个店铺，每个月，销售额最大的两笔
    val window1 = Window.partitionBy('shop_id, 'month).orderBy('amount desc)
    df.select('shop_id, 'month, 'amount, row_number() over window1 as "rn")
      .where('rn <= 2)
    /*.show()*/

    // 窗口函数： 求每个店的，每个月，月总额，累计到当月的累计额
    val window2 = Window.partitionBy('shop_id).orderBy('month).rowsBetween(Window.unboundedPreceding, Window.currentRow)
    df.groupBy('shop_id, 'month)
      .agg(sum('amount) as "month_amount")
      .select('shop_id, 'month, 'month_amount, sum('month_amount).over(window2) as "accu_amount")
      .show()


    spark.close()

  }
}
