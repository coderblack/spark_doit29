package cn.doitedu.spark.course

import org.apache.spark.{SparkConf, SparkContext}

import java.sql.DriverManager
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object C21_闭包引用 {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf(true)
    conf.setMaster("local")
    conf.setAppName("")
    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(Seq(1, 2, 3, 4, 5, 6, 7))

    val lst = new ListBuffer[Int]()  //   lst对象在driver程序中创建

    rdd.map(e => {
      // 算子内部函数中引用 lst
      // 其实是driver中那个lst经过序列化传到executor上后，又被反序列化出来的lst
      // 对算子内部引用的这个lst不论做什么修改，都不会影响到driver端那个lst
      lst += e
      println(lst)
      e
    }).count()

    println(lst)   // ListBuffer()


    /**
     * 能不能利用这个闭包引用机制来实现map端join ？？？
     */
    // 设法去获取小表数据，封装到hashmap
    val conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/abc", "root", "123456")
    val statement = conn.prepareStatement("select  *   from b")
    val resultSet = statement.executeQuery()

    val hashMap = new mutable.HashMap[Int, ExtraStu]()
    while(resultSet.next()){
      val id = resultSet.getInt(1)
      val city = resultSet.getString(2)
      val phone = resultSet.getString(3)
      hashMap.put(id,ExtraStu(id,city,phone))
    }

    // 构建RDD的DAG图，触发job
    val res = sc.textFile("data/wordcount/input/wc2.txt")
      .map(line=>{
        val splits = line.split(",")
        // 根据id去查询小表
        // 直接在算子内引用了driver中的hashmap，底层本质上是driver的hashmap封装在task中一起被序列化发送到executor上执行
        val stu = hashMap.get(splits(0).toInt).get
        line +","+stu.city + "," + stu.phone
      })

    res.saveAsTextFile("data/join4")
    sc.stop()

  }

}
