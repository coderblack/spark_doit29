package cn.doitedu.rdd_moni

import java.io.{BufferedReader, FileReader}

case class Person(id: Int, name: String, term: String, score: Int)

class MyFileRDD(path: String) {

  val innerIterator = new Iterator[String] {
    private val br = new BufferedReader(new FileReader(path))
    var line: String = null

    override def hasNext: Boolean = {
      line = br.readLine()
      line != null
    }

    override def next(): String = line
  }

  def foreach(f: String => Unit): Unit = {
    innerIterator.foreach(f)
  }

  def map(f:String=>MyPerson):MyMapRDD = {
    val newIterator = innerIterator.map(f)
    new MyMapRDD(newIterator)
  }

}
