package cn.doitedu.course

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

case class BaseStu(id:Int, name:String, term:String, score:Int)
case class ExtraStu(id:Int,phone:String,city:String)

object C20_广播变量 {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf
    conf.setMaster("local")
    conf.setAppName("")
    val sc = new SparkContext(conf)

    // 1,zs,doit29,80
    val rdd1 = sc.textFile("data/wordcount/input/wc2.txt")

    // 1,13866554776,上海
    val rdd2 = sc.textFile("data/wordcount/input/wc4.txt")

    /**
     * shuffle  JOIN
     * 如果 两表数据分布倾斜（比如某个或某几个id的数据特别多）
     */
    def shuffleJoin(): Unit ={
      // SQL:  select  rdd1.*,rdd2.*  from rdd1 join rdd2 on rdd1.id=rdd2.id
      val baseInfo: RDD[(Int, BaseStu)] = rdd1.map(line=>{
        val arr = line.split(",")
        (arr(0).toInt,BaseStu(arr(0).toInt,arr(1),arr(2),arr(3).toInt))
      })

      val extraInfo: RDD[(Int, ExtraStu)] = rdd2.map(line=>{
        val arr = line.split(",")
        (arr(0).toInt,ExtraStu(arr(0).toInt,arr(1),arr(2)))
      })

      val joined = baseInfo.join(extraInfo)
      val res = joined.map(tp=>s"${tp._2._1.id},${tp._2._1.name},${tp._2._1.term},${tp._2._1.score},${tp._2._2.phone},${tp._2._2.city}")

      res.saveAsTextFile("data/join_res")
    }


    /**
     * mapSide  JOIN
     * 思想： 让每个task持有小表的完整数据，而将大笔作为正常流程的流表来处理
     * 具体实现手段：
     *      spark中可以用缓存文件  "spark-submit --file  小表文件"
     *      spark中还有更方便的手段：利用广播变量（本质就是不用传文件让task去加载，而是直接将小表数据放在一个集合中，传给task）
     *
     */










    sc.stop()
  }

}
