package cn.doitedu.course

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.io.{BufferedReader, InputStreamReader}
import java.sql.DriverManager
import scala.collection.mutable

case class BaseStu(id:Int, name:String, term:String, score:Int)
case class ExtraStu(id:Int,phone:String,city:String)

object C20_广播变量与Join方式 {
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
    def mapSideJoin1():Unit={

      // 先将小表数据装入一个HashMap中
      val array: Array[String] = rdd2.collect()
      val extraInfoHashMap = array.map(line => {
        val arr = line.split(",")
        (arr(0), ExtraStu(arr(0).toInt, arr(1), arr(2)))
      }).toMap

      //  把小表的hashmap的对象，广播出去（广播给每一个将来的Executor进程）
      val bc = sc.broadcast(extraInfoHashMap)

      // 处理流表（大表）
      val res = rdd1.map(line=>{
        // 从广播变量中获取小表hashmap
        val hashMap = bc.value

        // 1,zs,doit29,80
        val id = line.split(",")(0)
        val extraStu = hashMap.get(id).get

        line + ","+extraStu.phone + ","+ extraStu.city
      })

      res.saveAsTextFile("data/mapjoin/")

    }


    /**
     * 与上面的方法逻辑一样
     * 区别是： 构造小表hashmap的来源、手段不同
     * 要求，小表的数据从一个mysql服务器的表来 abc.b表
     */
    def mapSideJoin2():Unit = {

      // 设法去获取小表数据
      val conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/abc", "root", "123456")
      val statement = conn.prepareStatement("select * from b")
      val resultSet = statement.executeQuery()

      // var extraInfoMap = Map.empty[Int,ExtraStu]
      val extraInfoMap = new mutable.HashMap[Int, ExtraStu]()
      // 从jdbc的返回结果中获取数据不断封装成bean对象，然后放入hashmap
      while(resultSet.next()){
        val id = resultSet.getInt(1)
        val city = resultSet.getString(2)
        val phone = resultSet.getString("score")
        extraInfoMap.put(id,ExtraStu(id,phone,city))
      }

      // 将封装小表数据的 hashmap  广播出去
      val bc = sc.broadcast(extraInfoMap)


      val res = rdd1.mapPartitions(iter=>{
        val 小表Map = bc.value

        iter.map(line=>{
          // 切分大表数据
          val splits = line.split(",")

          // 根据id去小表map中查询小表数据
          val extraStu = 小表Map.get(splits(0).toInt).get

          // 拼接两表的数据返回
          line + ","+ extraStu.phone + "," + extraStu.city

        })
      })

      res.saveAsTextFile("data/join2/")


      sc.stop()
    }


    /**
     * 与上面的方法逻辑一样
     * 区别是： 构造小表hashmap的来源、手段不同
     * 要求，小表的数据是在hdfs的文件中
     */
    def mapSideJoin3():Unit = {
      // 设法从HDFS去获取小表数据
      val fs = FileSystem.get(new Configuration())  // hdfs的客户端对象
      val in = fs.open(new Path("hdfs://doit01:8020/wc4/wc4.txt"))
      val br = new BufferedReader(new InputStreamReader(in))
      var line:String = null

      // 读文件，将数据封装到hashmap中
      val extraInfoMap = new mutable.HashMap[Int, ExtraStu]()
      do{
        line = br.readLine()
        val splits = line.split(",")
        extraInfoMap.put(splits(0).toInt,ExtraStu(splits(0).toInt,splits(1),splits(2)))
      }while( line!=null)

      // 广播出去
      val bc = sc.broadcast(extraInfoMap)


      // 构建DAG，提交job
      val res = rdd1.map(line=>{
        // 从广播变量获取小表数据hashMap
        val 小表数据HashMap = bc.value

        // 从大表数据获取用户id,根据id查询小表数据
        val splits = line.split(",")
        val extraStu = 小表数据HashMap.get(splits(0).toInt).get

        // 拼接结果
        line + ","+ extraStu.phone + "," + extraStu.city
      })
      res.saveAsTextFile("data/join3")
    }

  }

}
