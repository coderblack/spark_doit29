import java.io.{File, FileOutputStream, FileWriter, ObjectOutputStream, OutputStreamWriter}

object FileWr {
  def main(args: Array[String]): Unit = {
    val gn = new Array[Array[Long]](1)
    for(i <- 0 until 1){
      val g1 = new Array[Long](1024 * 1024 * 1024 / 8)
      val g11 = g1.zipWithIndex.map(_._2.toLong)
      gn(i) = g11
    }

    val stream = new ObjectOutputStream(new FileOutputStream(new File(("d:/ooo.arr"))))
    stream.writeObject(gn)

    stream.close()
  }

}
