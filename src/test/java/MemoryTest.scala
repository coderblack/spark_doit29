import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object MemoryTest {

  def main(args: Array[String]): Unit = {

    // 代码中临时设置日志打印级别
    // Logger.getLogger("org").setLevel(Level.WARN)

    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("wordCount")

    val sc = new SparkContext(conf)

  }

}
