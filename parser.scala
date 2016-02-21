import scala.io.Source

    var tempList = Seq()

    val filename = "data.txt"
    val lines = Source.fromFile(filename).getLines().to[Seq]
    //println(line.takeRight(5))
    //println(line.dropRight(line.length - 5))
    println(lines.map(parseLine))

  
  /**
 * Parsing
 */
def parseLine(line: String): Map[String, String] = {

  case class Field(
    val start: Int,
    val end: Int,
    val length: Int,
    val name: String,
    val description: String)


 

  val fields = Seq(
    Field(1, 4, 4, "var_length", "Length"),
    Field(5, 10, 6, "usaf_id", "Id"),
    Field(11, 15, 5, "wban", "Wban"),
    Field(16, 23, 8, "date", "Date"),
    Field(24, 27, 4, "gmt", "GMT"),
    Field(28, 28, 1, "data source", "Data Source"),
    Field(29, 34, 6, "lat", "Lat"),
    Field(35, 41, 7, "long", "Longitude"),
    Field(42, 46, 5, "report Type", "Report Type"),
    Field(47, 51, 5, "elev", "Elevation"),
    Field(52, 56, 5, "call letters", "Precipitation 1-Hour (mm)"),
    Field(57, 60, 4, "precip 6h", "Precipitation 6-Hour (mm)"),
    Field(61, 63, 3, "wind_dir", "Wind Direction"),
    Field(64, 64, 1, "wind dir flag", "Wind Direction Flag"),
    Field(65, 65, 1, "wind type", "Wind Type"),
    Field(66, 69, 4, "wind speed", "Wind Speed"),
    Field(70, 70, 1, "wind speed flag", "Wind Speed Flag"),
    Field(71, 75, 5, "sky_ceiling", "Sky Ceiling"),
    Field(76, 76, 1, "sky_ceiling_flag", "Sky Ceiling Flag"),
    Field(77, 77, 1, "sky_ceiling_determ", "Sky Ceiling Determ"),
    Field(79, 84, 6, "visibility", "Visibility"),
    Field(85, 85, 1, "visibility_flag", "Visibility Flag"),
    Field(86, 86, 1, "visibility_var", "Visibility Var"),
    Field(87, 87, 1, "visibility_var_flag", "Visibility Var Flag"),
    Field(88, 92, 5, "air_temp", "Temperature"),
    Field(93, 93, 1, "air_temp_flag", "Temperature Flag"),
    Field(94, 98, 5, "dew_point", "Dew Point"),
    Field(99, 99, 1, "dew_point_flag", "Dew Point Flag"),
    Field(100, 104, 5, "sea_lev", "Sea Level Press"),
    Field(105, 105, 1, "sea_lev_flag", "Sea Level Flag"))

  val empty = Map[String, String]().withDefaultValue("")

  if (line.length >= 61) {
    fields.foldLeft(empty) { (m, f) =>
      val value = line.substring(f.start - 1, f.end).trim
      val recoded = if (value == "-9999") "" else value
      //if (f.name =="air_temp") 
      m.updated(f.name, recoded)
    }
  } else {
    empty
  }

  // def stringToTemp(value:String): Temperature={
  //   return new Temperature()
  // }
}





  //   class Temperature{
  //    public String sign
  //    public Int temp
  //   public Temperature(String s, Int v){
  //   sign=s
  //   temp=v
  // }
//}


