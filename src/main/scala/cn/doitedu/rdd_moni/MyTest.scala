package cn.doitedu.rdd_moni

import java.sql.{DriverManager, ResultSet}

object MyTest {
  def main(args: Array[String]): Unit = {

    val sc = new MySparkContext()

    val rdd1 = sc.textFile("data/wordcount/input/wc2.txt")

    val rdd2: MyMapRDD = rdd1.map(line=>{
      val arr = line.split(",")
      MyPerson(arr(0).toInt,arr(1),arr(2),arr(3).toInt)
    })

    rdd2.foreach(println)


    println("----------------------")

    val getConn = ()=> DriverManager.getConnection("jdbc:mysql://localhost:3306/abc","root","123456")
    val mapRow1 = (resultSet:ResultSet) => {
      val id = resultSet.getInt(1)
      val age = resultSet.getString(2)
      val name = resultSet.getString(3)
      val score = resultSet.getInt(4)
      MyPerson(id,name,age+"",score)
    }

    val mapRow2 = (resultSet:ResultSet)=>{
      val id = resultSet.getInt(1)
      val age = resultSet.getString(2)
      val name = resultSet.getString(3)
      val score = resultSet.getInt(4)
      s"$id:$name:$age:$score"
    }



    val rdd3 = new MyJdbcRdd[String](getConn, "select id,age,name,score from stu where id>=? and id<=?", 1, 5,mapRow2)
    rdd3.foreach(println)



  }

}
