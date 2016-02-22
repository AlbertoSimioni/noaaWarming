import scala.io.Source
var tempList = Seq()

val filename = "010010-99999-2016"
val lines = Source.fromFile(filename).getLines().to[Seq]
    //println(line.takeRight(5))
    //println(line.dropRight(line.length - 5))
    //println(lines.map(parseLine))
lines.map(parseLine)

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
    Field(11, 15, 5, "wban", "Wban"), //Another identifier of the station
    Field(16, 23, 8, "date", "Date"), //YYYYMMDD format
    Field(24, 27, 4, "gmt", "GMT"), //HHMM   HH:[0,23]
    Field(28, 28, 1, "data source", "Data Source"), //The type of the data source
    Field(29, 34, 6, "lat", "Lat"), //MIN: -90000 MAX: +90000  UNITS: Angular Degrees
    Field(35, 41, 7, "long", "Longitude"), //MIN: -179999 MAX: +180000 UNITS: Angular Degrees
    Field(42, 46, 5, "report Type", "Report Type"),
    Field(47, 51, 5, "elev", "Elevation"), //MIN: -0400 MAX: +8850
    Field(52, 56, 5, "call letters", "Precipitation 1-Hour (mm)"),
    Field(57, 60, 4, "precip 6h", "Precipitation 6-Hour (mm)"), //quality control process name
    Field(61, 63, 3, "wind_dir", "Wind Direction"), //MIN: 001 MAX: 360 UNITS: starting from true north
    Field(64, 64, 1, "wind dir flag", "Wind Direction Flag"), //direction quality code
    Field(65, 65, 1, "wind type", "Wind Type"), //Descriptor of the wind
    Field(66, 69, 4, "wind speed", "Wind Speed"), //MIN: 0000 MAX: 0900 UNITS: meters per second
    Field(70, 70, 1, "wind speed flag", "Wind Speed Flag"), //quality
    Field(71, 75, 5, "sky_ceiling", "Sky Ceiling"),//MIN: 00000 MAX: 22000 UNITS: Meters
    Field(76, 76, 1, "sky_ceiling_flag", "Sky Ceiling Flag"), //quality
    Field(77, 77, 1, "sky_ceiling_determ", "Sky Ceiling Determ"),//how was determined the ceiling
    Field(78, 78, 1, "cavok_code", "Sky Condition Observation CAVOK code"),
    Field(79, 84, 6, "visibility", "Visibility"), //MIN: 000000 MAX: 160000 UNITS: Meters
    Field(85, 85, 1, "visibility_flag", "Visibility Flag"),//quality
    Field(86, 86, 1, "visibility_var", "Visibility Var"), //if the visibility is variable
    Field(87, 87, 1, "visibility_var_flag", "Visibility Var Flag"), //quality
    Field(88, 92, 5, "air_temp", "Temperature"), //MIN: -0932 MAX: +0618 UNITS: Degrees Celsius
    Field(93, 93, 1, "air_temp_flag", "Temperature Flag"),//quality
    Field(94, 98, 5, "dew_point", "Dew Point"), //MIN: -0982 MAX: +0368 UNITS: Degrees Celsius
    Field(99, 99, 1, "dew_point_flag", "Dew Point Flag"), //qaulity
    Field(100, 104, 5, "sea_lev", "Sea Level Press"), //MIN: 08600 MAX: 10900 UNITS: Hectopascals
    Field(105, 105, 1, "sea_lev_flag", "Sea Level Flag")) //qaulity

  val empty = Map[String, String]().withDefaultValue("")
  val output = scala.tools.nsc.io.File("output.txt");
  if (line.length >= 61) {
    fields.foldLeft(empty) { (m, f) =>
      val value = line.substring(f.start - 1, f.end).trim
      val recoded = if (value == "-9999") "" else value
      if (f.name =="air_temp"){
        val sign=line.substring(f.start - 1, f.start).trim
        val temp=line.substring(f.start, f.end).trim.toInt
        output.appendAll(f.name + ": "+ sign + " " + temp + "\n" );
      //println(sign + " " + temp)
    }
      output.appendAll(f.name + ": "+ value +"\n")
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


