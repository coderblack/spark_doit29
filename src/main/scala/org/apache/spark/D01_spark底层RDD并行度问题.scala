package org.apache.spark

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.FileSplit
import org.apache.hadoop.mapred.lib.{CombineFileSplit, CombineTextInputFormat}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.{HadoopPartition, JdbcPartition, JdbcRDD, ParallelCollectionPartition, RDD}

import java.sql.DriverManager

case class Order(oid: Int, uid: String, amt: Double, payType: String)


object D01_spark底层RDD并行度问题 {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.WARN)

    val conf = new SparkConf()
    conf.setAppName("")
    conf.setMaster("local[1]") // 用4核来做分布式的模拟运行
    //conf.set("spark.default.parallelism","2")  // 设置了默认并行度参数
    val sc = new SparkContext(conf)

    // jdbcRDD(sc)
    // parallelizedRDD(sc)
    // 衍生1对1依赖RDD分区数演示(sc)
    //衍生Coalesce依赖RDD分区数演示(sc)
    宽依赖RDD分区数演示(sc)

    sc.stop()
  }

  /**
   * 1. 文件类数据源RDD的分区数决定演示
   * 默认由TextInputFormat决定分区机制
   */
  def textFileRdd(sc: SparkContext): Unit = {
    val rdd: RDD[String] = sc.textFile("data/parallel/input")

    val partitions: Array[Partition] = rdd.partitions
    for (par <- partitions) {
      val hadoopPartition = par.asInstanceOf[HadoopPartition]
      val split = hadoopPartition.inputSplit.value.asInstanceOf[FileSplit]
      val partitionId = par.index
      val path = split.getPath
      val start = split.getStart
      val length = split.getLength
      // 可以看出：一个文件至少被分成1个partition；如果一个文件较大，则还会按照blockSize划分成多个partition
      println(s"$partitionId,$path,$start,$length")
    }
  }


  /**
   * 2. 文件类数据源RDD的分区数决定演示
   * 指定InputFormat为CombineTextInputFormat，则分区决定机制有CombineTextInputFormat决定
   */
  def textFileRdd2(sc: SparkContext): Unit = {

    val rdd1: RDD[(LongWritable, Text)] = sc.hadoopFile("data/parallel/input/", classOf[CombineTextInputFormat], classOf[LongWritable], classOf[Text])
    val rdd2: RDD[String] = rdd1.map(pair => pair._2.toString)
    val partitions2 = rdd2.partitions

    for (par <- partitions2) {
      val hadoopPartition = par.asInstanceOf[HadoopPartition]
      val partitionId = par.index

      val split: CombineFileSplit = hadoopPartition.inputSplit.value.asInstanceOf[CombineFileSplit]

      val paths = split.getPaths
      val offsets = split.getStartOffsets
      val lengths = split.getLengths

      // 可以看出，一个partition将包含多个文件
      println(s"${partitionId},${paths.mkString(":")},${offsets.mkString(":")},${lengths.mkString(":")}")

    }
  }


  /**
   * 3. JdbcRDD的分区数决定演示
   */
  def jdbcRDD(sc: SparkContext): Unit = {
    val conn = () => DriverManager.getConnection("jdbc:mysql://localhost:3306/abc", "root", "123456")
    val sql = "select * from stu where id>=? and id<=?"
    val rdd = new JdbcRDD(sc, conn, sql, 1, 100, 2)

    // 打印rdd的分区信息
    val partitions: Array[Partition] = rdd.partitions
    for (par <- partitions) {
      val p = par.asInstanceOf[JdbcPartition]
      println(s"${p.index},${p.lower},${p.upper}")
    }

  }


  /**
   * 4. parallelizedRDD的分区数决定演示
   */
  def parallelizedRDD(sc: SparkContext): Unit = {
    //val rdd = sc.makeRDD(Seq(1, 2, 3, 4, 5, 6),4)


    // 如果没有传入分区数
    // 则它用做分区数：
    //  本地运行模式下：   scheduler.conf.getInt("spark.default.parallelism", totalCores)
    //  分布式运行模式下：  conf.getInt("spark.default.parallelism", math.max(totalCoreCount.get(), 2))  // 默认并行度参数没有设置的情况下，最少为2
    val rdd = sc.makeRDD(Seq(1, 2, 3, 4, 5, 6))

    // 打印分区信息
    val partitions: Array[Partition] = rdd.partitions
    for (par <- partitions) {
      // 并行化集合产生的RDD的分区：每个分区就是代表原来集合的一部分数据值
      val p: ParallelCollectionPartition[Int] = par.asInstanceOf[ParallelCollectionPartition[Int]]
      println(s"${p.index},${p.values}")
    }

  }


  /**
   * 衍生RDD分区数
   * |-------------|         |-------------|
   * |             |         |             |
   * |   父RDD     | =算子=>  |   本RDD     |
   * |             |         |             |
   * |-------------|         |-------------|
   *
   *    1. 如果本RDD与父RDD之间是一对一窄依赖，则  本RDD的分区数=父RDD的分区数，且各分区之间是一对一关系
   *       2. 如果本RDD与父RDD之间是宽依赖，则前后分区数没有逻辑联系，本RDD的分区数到底是几？
   */
  def 衍生1对1依赖RDD分区数演示(sc: SparkContext): Unit = {

    val rdd1 = sc.makeRDD(Seq(1, 2, 3, 4, 5, 6, 7, 8), 2)

    /**
     * map算子，mapPartitions算子，filter算子 ……
     * 这些算子都是产生 OneToOne依赖
     * 所以，子RDD的分区数 == 父RDD的分区数
     */
    val rdd_map = rdd1.map(_ * 10)
    println("map算子产生的RDD的分区数等于父RDD的分区数： " + rdd_map.partitions.length)

  }

  def 衍生Range依赖RDD分区数演示(sc: SparkContext): Unit = {
    /**
     * union所产生的父子RDD之间的依赖叫做： RangeDependency
     * 而RangeDependency的本质依然是一个 “一对一”依赖
     * （只不过不像简单的一对一那样，通过本rdd分区id直接==父rdd分区id，而是需要做一个计算才能找到本rdd的某分区对应的是哪个父rdd的哪个分区）
     *
     * union产生的rdd的分区数为所有父RDD的分区数之和
     *
     * rdd1:          0   1
     * rdd2:                 0   1   3
     * rdd_union:     0   1  2   3   4
     */
    val rdd1 = sc.makeRDD(Seq(1, 2, 3, 4, 5, 6, 7, 8), 2)
    val rdd2 = sc.makeRDD(Seq(2, 3, 3), 3)
    val rdd_union = rdd1.union(rdd2)
    println(s"union产生的rdd的分区数为所有父RDD的分区数之和：${rdd_union.partitions.length}")

  }


  /**
   * 在CoalescedRDD中，封装的依赖描述对象为：窄依赖的一个匿名实现：
   * new NarrowDependency(prev) {
   * def getParents(id: Int): Seq[Int] =
   * partitions(id).asInstanceOf[CoalescedRDDPartition].parentsIndices
   * }
   */
  def 衍生Coalesce依赖RDD分区数演示(sc: SparkContext): Unit = {
    val rdd = sc.makeRDD(Seq(1, 2, 3, 4, 5, 6, 7, 8, 9), 4)
    val rdd2 = rdd.mapPartitionsWithIndex((idx, iter) => iter.map(x => s"初始分区号:$idx - ${x}"))
    rdd2.foreach(println)


    // coalesce是一个用于改变rdd分区数的算子
    // coalesce 底层就是把父RDD的若干个分区，合并成子RDD的一个分区
    // 子RDD的一个分区合并哪几个父RDD分区，是由 分区合并器来计算的；
    // 分区合并器是一个算法；它可以由用户自定义并传入；
    // 如果用户没有传入分区合并器，则会使用spark内置的 DefaultPartitionCoalescer
    // 而这个默认分区合并器的算法，主要考虑两点：
    //     ① 合并的分区数的均衡性！
    //     ② 数据的物理位置相同的父分区尽可能合并给同一个子分区
    val rdd_coalesced = rdd2.coalesce(3, false) // coalesce可以把分区数变大（必须设置shuffle=true），也可以变小

    rdd_coalesced.mapPartitionsWithIndex((idx, iter) => iter.map((idx, _))).foreach(println)

    // repartition算子，也是改变分区数的算子
    // repartition 底层调用的是： => coalesce(numPartitions, shuffle = true)
    // 所以repartition可以把分区改大也可以改小，而且产生的永远是：Shuffle依赖
    val rdd3 = rdd2.repartition(8) // rdd3的分区数是： 8
    val rdd4 = rdd2.repartition(2) // rdd3的分区数是： 2


  }


  def 宽依赖RDD分区数演示(sc: SparkContext): Unit = {
    val rdd1: RDD[Order] = sc.makeRDD(Seq(
      Order(1, "u01", 50.0, "wx"),
      Order(2, "u03", 60.0, "zfb"),
      Order(3, "u02", 20.0, "wx"),
      Order(4, "u02", 40.0, "zfb"),
      Order(5, "u01", 30.0, "zfb"),
      Order(6, "u03", 50.0, "wx")

    ), 4)

    // rdd2 与 rdd1 之间是宽依赖
    // 宽依赖的子RDD的分区数，都可以由调用shuffle类算子时人为指定

    // 传入分区数，在底层也会自动传入一个  HashPartitioner(分区数）
    val rdd2 = rdd1.groupBy(order=>order.payType, 2)

    // 传入分区器，同时决定了两件事：
    //     shuffle时候的分区规则，以及分区个数
    val rdd3 = rdd1.map(order => (order.payType, order))
      .groupBy(tp => tp._1, new HashPartitioner(3) {
        override def getPartition(key: Any): Int = super.getPartition(key)
      })


    // 分区数和分区器都没有传
    // 底层还是会帮我们传入一个分区器   groupBy[K](f, defaultPartitioner(this))
    val rdd4 = rdd1.groupBy(order => order.uid)


  }


}