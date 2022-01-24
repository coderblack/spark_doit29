package cn.doitedu.spark.course

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

case class BaseInfo(id:Int,name:String,age:Int)
case class ExtraInfo(id:Int,city:String,score:Int)


object C14_RDD算子_关联算子 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf = new SparkConf
    conf.setAppName("")
    conf.setMaster("local")

    val sc = new SparkContext(conf)

    val rdd1: RDD[(Int, String, Int)] = sc.makeRDD(Seq(
      (1, "zs", 18),
      (1, "zs", 19),
      (2, "ls", 20),
      (3, "ww", 26),
      (4, "tq", 25),
      (5, "wb", 35),
    ))

    val rdd2: RDD[(Int, String, Int)] = sc.makeRDD(Seq(
      (1, "北京", 500),
      (1, "西安", 480),
      (2, "北京", 496),
      (3, "上海", 635),
      (4, "上海", 360),
      (6, "西安", 660)
    ))

    // join(rdd1,rdd2)
    // union(rdd1,rdd2)
    subtract(sc)


    sc.stop()
  }

  /**
   * coGroup演示
   * @param rdd1
   * @param rdd2
   */
  def coGroup(rdd1:RDD[(Int,String,Int)],rdd2:RDD[(Int,String,Int)]): Unit ={
    // 先将上面两个rdd变成  以 id为key的  KV RDD
    val rddA = rdd1.map(tp => (tp._1, tp))
    /**
     *     (1, (1, "zs", 18))
           (1, (1, "zs", 19))
           (2, (2, "ls", 20))
           (3, (3, "ww", 26))
           (4, (4, "tq", 25))
     */
    val rddB = rdd2.map(tp => (tp._1, tp))
    /**
    (1, (1, "北京", 500))
           (1, (1, "西安", 480))
           (2, (2, "北京", 496))
           (3, (3, "上海", 635))
           (4, (4, "上海", 360))
     */

    /**
     * coGroup : 协同分组
     *   两个rdd，按照相同key，一起分组
     *   相同组的数据在结果RDD中会放在一行，且左右两个rdd的数据在这一行中是分别用一个迭代器表示
     */
    val rddC  = rddA.cogroup(rddB)
    /**
    (4,(CompactBuffer((4,tq,25)),CompactBuffer((4,上海,360))))
           (1,(CompactBuffer((1,zs,18), (1,zs,19)),CompactBuffer((1,北京,500), (1,西安,480))))
           (3,(CompactBuffer((3,ww,26)),CompactBuffer((3,上海,635))))
           (2,(CompactBuffer((2,ls,20)),CompactBuffer((2,北京,496))))
     */

    /**
     * 利用上面的coGroup后的结果，进行转化，得到join的效果
     */
    val joinedResult = rddC.flatMap(tp=>{
      val leftData: Iterable[(Int, String, Int)] = tp._2._1
      val rightData: Iterable[(Int, String, Int)] = tp._2._2

      val left: List[(Int, String, Int)] = leftData.toList
      val right: List[(Int, String, Int)] = rightData.toList


      // yield ：是用于在for循环中收集循环体的返回结果数据
      for(tpL <- left; tpR <- right) yield {
        (tpL._1,tpL._2,tpL._3,tpR._1,tpR._2,tpR._3)
      }
    })

    joinedResult.foreach(println)
  }

  /**
   * JOIN 演示
   */
  def join(rdd1:RDD[(Int,String,Int)],rdd2:RDD[(Int,String,Int)]):Unit = {
    // 把两个 分别变 KV RDD
    val rddA: RDD[(Int, BaseInfo)] = rdd1.map(tp => (tp._1, BaseInfo(tp._1,tp._2,tp._3)))
    val rddB: RDD[(Int, ExtraInfo)] = rdd2.map(tp => (tp._1, ExtraInfo(tp._1,tp._2,tp._3)))

    // 内连接
    val rddC1: RDD[(Int, (BaseInfo, ExtraInfo))] = rddA.join(rddB)
    rddC1.foreach(println)

    println("--------涛哥爱玩左外连接--------------")

    // 左外连接
    val rddC2 = rddA.leftOuterJoin(rddB)
    rddC2.foreach(println)


    println("--------涛哥喜欢右连接--------------")
    val rddC3 = rddA.rightOuterJoin(rddB)
    rddC3.foreach(println)


    println("--------涛哥喜欢左右逢源（全外连接）--------------")
    val rddC4 = rddA.fullOuterJoin(rddB)
    rddC4.foreach(println)
    /**
     * (6,(None,Some(ExtraInfo(6,西安,660))))
       (3,(Some(BaseInfo(3,ww,26)),Some(ExtraInfo(3,上海,635))))
       (5,(Some(BaseInfo(5,wb,35)),None))
       (2,(Some(BaseInfo(2,ls,20)),Some(ExtraInfo(2,北京,496))))
     */

    println("--------涛哥喜欢没条件--------------")
    val 笛卡尔积 = rdd1.cartesian(rdd2)
    笛卡尔积.foreach(println)

  }

  /**
   * union 演示
   * @param rdd1
   * @param rdd2
   */
  def union(rdd1:RDD[(Int,String,Int)],rdd2:RDD[(Int,String,Int)]):Unit = {
    val rddC = rdd1.union(rdd2)  // 不去重
    rddC.foreach(println)
  }

  def subtract(sc:SparkContext):Unit = {

    val rdd1 = sc.makeRDD(Seq(
      1,
      2,
      3,
      4
    ))
    val rdd2 = sc.makeRDD(Seq(
      1,
      3,
      5,
      7
    ))

    /** sql：
     * select
     *  a.id
     * from a left join b on a.id=b.id
     * where b.id is null
     */
    val rddC = rdd1.subtract(rdd2)
    rddC.foreach(println)


  }


}
