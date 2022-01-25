package cn.doitedu.sparksql

import cn.doitedu.sparksql.beans.JavaDoitStudent
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, Row, SparkSession}

import scala.beans.BeanProperty


case class DoitTeacher(id:Int,name:String,power:Double)

// 这种scala类，属性没有java中那种setter，JavaBean的Encoder就没法通过反射手段获取schema信息
class DoitStudent(var id:Int,val name:String,val gender:String) extends Serializable

// @BeanProperty 是一个scala中的注解，编译器看到这个注解，就会对被注解的成员变量生成与java语法相同的getter和setter
class DoitStudent2(@BeanProperty var id:Int, @BeanProperty var name:String, @BeanProperty var gender:String) extends Serializable

object C07_RDD转Dataset {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("")
      .master("local")
      .getOrCreate()

    val rdd1: RDD[Int] = spark.sparkContext.makeRDD(Seq(1, 2, 3, 4, 5, 6))

    // rdd[T]转成dataset（dataframe），需要对应的Encoder[T]
    // Encoder，就是帮我们将T类型数据自动解构出schema的工具，而且会包含底层序列化机制
    // 对于一些常见的数据类型T（比如，基本类型；元组类型，case class，product，java Bean，scala Bean），spark都自带了对应的Encoder
    val ds1 = spark.createDataset(rdd1)(Encoders.scalaInt)  // 手动传 Encoder[Int]

    // 其实，这个createDataset(rdd)(encoder)后面的encoder参数是一个隐式参数，那么调用时可以隐式传入
    // implicit  val enc = Encoders.scalaInt

    import spark.implicits._
    val ds11 = spark.createDataset(rdd1)   // 隐式传Encoder[Int]

    val rdd2: RDD[Double] = spark.sparkContext.makeRDD(Seq(1.0, 2.3, 4.5))
    val ds2 = spark.createDataset(rdd2)/*(Encoders.scalaDouble)*/

    val rdd3: RDD[Seq[Int]] = spark.sparkContext.makeRDD(Seq(Seq(1, 2, 3), Seq(1, 3, 4), Seq(4, 5, 6)))
    val ds3 = spark.createDataset(rdd3)
    // ds3.printSchema()  //  value:array<integer>
    // ds3.show()

    val rdd4: RDD[Map[String, Int]] = spark.sparkContext.makeRDD(Seq(Map("a" -> 1, "b" -> 2), Map("c" -> 1), Map("d" -> 2)))
    val ds4 = spark.createDataset(rdd4)
    // ds4.printSchema()  // value:map<string,integer>
    // ds4.show()

    val rdd5: RDD[(Int,String,Int)] = spark.sparkContext.makeRDD(Seq((1, "aa", 18), (2, "bb", 26), (3, "cc", 25)))
    // val ds5 = spark.createDataset(rdd5)
    // 其实，在上下文中如果引入了 spark.implicits._
    // 那么，rdd上就直接拥有了toDS()和toDF()方法
    val ds5: Dataset[(Int, String, Int)] = rdd5.toDS()
    val df5: Dataset[Row] = rdd5.toDF()
    // ds5.printSchema()  //  _1:integer, _2:string, _3:integer   // 解构出来的字段名，就是Tuple3类的成员变量名
    // ds5.show()


    val rdd6: RDD[DoitTeacher] = spark.sparkContext.makeRDD(Seq(DoitTeacher(1, "马云", 98.9), DoitTeacher(2, "马化腾", 109.9), DoitTeacher(3, "强子", 97.7)))
    val ds6 = spark.createDataset(rdd6)
    // ds6.printSchema()   // id:integer,  name:string, power:double
    // ds6.show()


    // javabean，可以用Encoders.bean(classOf[JavaBean])来对数据进行解构生成schema
    // 它底层是用反射手段来获取类中的成员属性名和属性类型
    // 它底层反射代码并不是直接反射属性名，而是反射属性的setter方法名，得到属性名
    val rdd7: RDD[JavaDoitStudent] = spark.sparkContext.makeRDD(Seq(new JavaDoitStudent(1, "宝洁", "male"), new JavaDoitStudent(2, "如梦", "female")))
    val ds7 = spark.createDataset(rdd7)(Encoders.bean(classOf[JavaDoitStudent]))
    // ds7.printSchema()
    // ds7.show()


    // 普通scala类的解构
    val rdd8: RDD[DoitStudent2] = spark.sparkContext.makeRDD(Seq(new DoitStudent2(1, "宝洁", "male"), new DoitStudent2(2, "如梦", "female")))
    val ds8 = spark.createDataset(rdd8)(Encoders.bean(classOf[DoitStudent2]))
    ds8.printSchema()
    ds8.show()









    spark.close()

  }
}
