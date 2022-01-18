package cn.doitedu.rdd_moni

class MySparkContext {

  def textFile(path:String):MyFileRDD = {
    new MyFileRDD(path)
  }

}
