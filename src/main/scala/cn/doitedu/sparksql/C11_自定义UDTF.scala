package cn.doitedu.sparksql

import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory
import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspector, ObjectInspectorFactory, PrimitiveObjectInspector, StructObjectInspector}
import org.apache.spark.sql.SparkSession

import java.util

object C11_自定义UDTF {
  def main(args: Array[String]): Unit = {

    /**
     *
     * 1,zs,1-3
     * 2,ls,2-3
     * 3,ww,1-4
     * 对上面的数据进行查询，得到如下结果：
     * 1,zs,1,4
     * 1,zs,2,4
     * 1,zs,3,4
     * 2,ls,2,5
     * 2,ls,3,5
     * 3,ww,1,5
     * 3,ww,2,5
     * 3,ww,3,5
     * 3,ww,4,5
     */

    val spark = SparkSession.builder()
      .appName("")
      .master("local")
      .enableHiveSupport()  // 启动hive功能
      .getOrCreate()
    import spark.implicits._

    val df = spark.createDataset(Seq(
      (1, "zs", "1-3"),
      (2, "ls", "2-3"),
      (3, "ww", "1-4")
    )).toDF("id","name","range")

    df.createTempView("df")

    // 注册自定义的UDTF函数 ： 用 hive 的自定义函数注册方式来注册
    spark.sql("create temporary function my_udtf as 'cn.doitedu.sparksql.MyExplode'")
    spark.sql(
      """
        |select
        |  id,
        |  name,
        |  my_udtf(range)
        |from df
        |
        |""".stripMargin).show()

    spark.close()
  }
}

class MyExplode extends GenericUDTF {

  // 对输入参数进行检查
  // 以及对输出结果进行结构定义（输出几个列，什么类型，什么列名）
  override def initialize(argOIs: Array[ObjectInspector]): StructObjectInspector = {
    if(argOIs.length != 1) throw new Exception("输出的字段个数不对，应该输入1个")
    if(!argOIs(0).getTypeName.equals("string"))  throw new Exception("输出的字段的数据类型不对，应该输入string类型的字段")

    // 构造输出结果的列名list
    val names = new util.ArrayList[String]()
    names.add("f1")   // 第一列，字段名为  f1
    names.add("f2")  // 第二列，字段名为  f2

    // 构造输出结果的列类型list
    val fields = new util.ArrayList[ObjectInspector]()
    fields.add(PrimitiveObjectInspectorFactory.javaIntObjectInspector)  // 第1列，是一个java 的int类型字段
    fields.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector)  // 第2列，是一个java 的String类型字段

    // 生成输出结构的定义对象
    ObjectInspectorFactory.getStandardStructObjectInspector(names,fields)
  }

  // 对一条输入的数据进行处理，输出多行多列
  override def process(objects: Array[AnyRef]): Unit = {
    // "1-3"
    val value = objects(0).asInstanceOf[String]
    // 切割 [1,3]
    val splits: Array[Int] = value.split("-").map(s=>s.toInt)

    // 输出
    val sumStr = (splits(0)+splits(1))+""
    for(i <- splits(0) to splits(1)){
      forward( Array(i,sumStr) ) // 输出一行（行中包含两列）
    }
  }

  override def close(): Unit = {}
}
