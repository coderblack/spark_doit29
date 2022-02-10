import ch.hsr.geohash.GeoHash

object GeohashTest {
  def main(args: Array[String]): Unit = {

    val str1 = GeoHash.geoHashStringWithCharacterPrecision(45.52835, 156.23857235, 5)
    val str4 = GeoHash.geoHashStringWithCharacterPrecision(45.52837, 156.23857285, 5)
    val str2 = GeoHash.geoHashStringWithCharacterPrecision(45.52435, 156.23257235, 5)
    val str3 = GeoHash.geoHashStringWithCharacterPrecision(60.52435, 160.23257235, 5)
    println(str1)
    println(str4)
    println(str2)
    println(str3)


  }

}
