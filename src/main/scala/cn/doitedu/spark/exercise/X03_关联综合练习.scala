package cn.doitedu.spark.exercise

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

case class Order(oid: Int, uid: String, amt: Double)

case class OrderItem(oid: Int, pid: String, num: Int, price: Double)

case class Product(pid: String, cid: String, brand: String)

object X03_关联综合练习 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf = new SparkConf
    conf.setAppName("")
    conf.setMaster("local")

    val sc = new SparkContext(conf)
    /**
     * 表1（订单汇总表）：订单id，用户id，订单总额
     * 1,u01,20
     * 2,u01,15
     * 3,u02,30
     * 4,u03,40
     */
    val orders: RDD[Order] = sc.makeRDD(Seq(
      Order(1, "u01", 20),
      Order(2, "u01", 15),
      Order(3, "u02", 30),
      Order(4, "u03", 40)
    ))

    /**
     * 表2（订单详情表）：订单id，商品id，购买件数，商品单价
     * 1,p01,1,10.0
     * 1,p03,2,30.0
     * 2,p04,2,40.0
     * 3,p02,2,20.0
     * 4,p01,1,10.0
     * 4,p05,2,50.0
     */
    val items: RDD[OrderItem] = sc.makeRDD(Seq(
      OrderItem(1, "p01", 1, 10.0),
      OrderItem(1, "p03", 2, 30.0),
      OrderItem(2, "p04", 2, 40.0),
      OrderItem(3, "p02", 2, 20.0),
      OrderItem(4, "p01", 1, 10.0),
      OrderItem(4, "p05", 2, 50.0)
    ))


    /**
     * 表3（商品信息表）：商品id，品类id，品牌id
     * p01,c01,b01
     * p02,c01,b01
     * p03,c02,b03
     * p04,c02,b01
     * p05,c01,b02
     */
    val products = sc.makeRDD(Seq(
       Product("p01","c01","b01"),
       Product("p02","c01","b01"),
       Product("p03","c02","b03"),
       Product("p04","c02","b01"),
       Product("p05","c01","b02")
    ))


    /**
     * 统计如下报表：
     *    1. 每个品类的订单总额（按原价）
     *    select
     *         products.cid,
     *         sum(items.num*items.price)  as amt
     *    from items join products  on items.pid = products.pid
     *    group by products.cid
     *
     *
     *    2. 每个用户的订单总额（按原价）
     *    3. 每个品牌的订单总额（按原价）
     *    4. 每个订单的优惠总额
     *
     *    SELECT
     *       oid,
     *       o1.origin_amt - orders.amt  as discount
     *    FROM
     *      (select
     *        oid,
     *        sum(num*price) as origin_amt
     *      from items
     *      group by oid ) o1
     *    JOIN  orders  ON o1.oid=orders.oid
     *
     */
    def demand1(): Unit ={
      val orderItemsKV = items.map(orderItem => (orderItem.pid, orderItem))
      val productsKV = products.map(product => (product.pid, product))

      val joinedTmp :RDD[(String,(OrderItem,Product))]= orderItemsKV.join(productsKV)

      // 把上面的嵌套结构，变成一层（购买数量，商品单价，商品品类）
      val joined: RDD[(Int, Double, String)] = joinedTmp.map(tp => (tp._2._1.num, tp._2._1.price, tp._2._2.cid))

      // 计算同品类的总额 [品类id,相同品类的一组数据]
      val grouped :RDD[(String,Iterable[(Int,Double,String)])]= joined.groupBy(_._3)

      // 为每个品类，算总额
      val res: RDD[(String, Double)] = grouped.map(tp=>{
        val cid = tp._1
        val amt = tp._2.map(tp => tp._1 * tp._2).sum
        (cid,amt)
      })

      res.foreach(println)

      // collect是将整个rdd的所有数据收集成一个本地集合
      //val resArray: Array[(String, Double)] = res.collect()
    }

    def demand4():Unit = {
      // 先对items表，按oid，聚合原价订单总额
      val grouped:RDD[(Int,Iterable[OrderItem])] = items.groupBy(_.oid)

      // 聚合每个订单的原价总额
      val orderOriginAmt: RDD[(Int, Double)] = grouped.map(tp=>{
        val oid = tp._1
        val originAmt = tp._2.map(orderItem => orderItem.num * orderItem.price).sum
        (oid,originAmt)
      })

      // 将聚合好的订单原价总额，  关联   订单汇总信息表
      val ordersKV: RDD[(Int, Order)] = orders.map(order => (order.oid, order))
      //             [oid, （左表数据, 右表数据)]
      val joined :RDD[(Int,(Double,Order))]= orderOriginAmt.join(ordersKV)

      // 拿 原价总额 - 实付总额
      val res = joined.map(tp => (tp._1, tp._2._1 - tp._2._2.amt))

      res.foreach(println)
    }


    demand4()

    sc.stop()
  }
}
