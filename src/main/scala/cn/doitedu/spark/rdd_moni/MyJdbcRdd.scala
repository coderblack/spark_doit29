package cn.doitedu.spark.rdd_moni

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

class MyJdbcRdd[U](getConn:()=>Connection,sql:String,p1:Int, p2:Int,mapRow: ResultSet=>U) {

  val innerIterator = new Iterator[U]{
    private val conn: Connection = getConn()
    private val statement: PreparedStatement = conn.prepareStatement(sql)
    statement.setInt(1,p1)
    statement.setInt(2,p2)
    private val resultSet: ResultSet = statement.executeQuery()


    override def hasNext: Boolean = resultSet.next()

    override def next(): U = {
      mapRow(resultSet)
    }
  }


  def foreach(f:U=>Unit):Unit = {
    innerIterator.foreach(f)
  }


  def map(f:U=>MyPerson):MyMapRDD = {
    new MyMapRDD(innerIterator.map(f))
  }

}
