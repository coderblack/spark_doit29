package cn.doitedu.rdd_moni

class MyMapRDD(var innerIterator:Iterator[MyPerson]){

  def foreach(f: MyPerson => Unit): Unit = innerIterator.foreach(f)

}
