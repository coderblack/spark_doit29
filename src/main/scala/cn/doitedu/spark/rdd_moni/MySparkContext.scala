package cn.doitedu.spark.rdd_moni

class MySparkContext {

  def textFile(path:String):MyFileRDD = {
    new MyFileRDD(path)
  }

}
