import scala.io.Source
var tempList = Seq()

val filename = "010010-99999-2016"
val lines = Source.fromFile(filename).getLines().to[Seq]

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
    Field(52, 56, 5, "call letters", ""),
    Field(57, 60, 4, "qc_level", "Quality control"), //quality control process name
    Field(61, 63, 3, "wind_dir", "Wind Direction"), //MIN: 001 MAX: 360 UNITS: starting from true north
    Field(64, 64, 1, "wind dir flag", "Wind Direction Flag"), //direction quality code
    Field(65, 65, 1, "wind_type", "Wind Type"), //Descriptor of the wind
    Field(66, 69, 4, "wind_speed", "Wind Speed"), //MIN: 0000 MAX: 0900 UNITS: meters per second
    Field(70, 70, 1, "wind_speed_flag", "Wind Speed Flag"), //quality
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
      var fieldValue = line.substring(f.start - 1, f.end).trim
      //val recoded = if (value == "-9999") "" else value // what is this line for?
      if (f.name =="air_temp" || f.name ==  "wind_speed"){
          if(fieldValue != "9999"){
              fieldValue=((fieldValue.toInt)/10.0).toString
          }
          else fieldValue = ""
      }

      if (f.name == "lat" || f.name == "long") fieldValue=((fieldValue.toInt)/1000.0).toString

      output.appendAll(f.name + ": "+ fieldValue +"\n")

      m.updated(f.name, fieldValue)
    }
  }
  var additionalFields = line.substring(108)
  val aa1pos = additionalFields.indexOf("AA1",0)
  if(aa1pos != -1 ){
      var prechrs = additionalFields.substring(aa1pos+3, aa1pos+5)
      if(prechrs == "99"){
        prechrs = ""
      }
      var precdep = (additionalFields.substring(aa1pos+5, aa1pos+9))
      if(precdep != "9999"){
         precdep = (precdep.toInt/10.0).toString
      }
      else precdep = "";
      //liquid precipitations
      empty.updated("prechrs",prechrs) //MIN: 00 MAX: 98 UNITS: Hours (measured hours)
      empty.updated("precdep",precdep) //MIN: 0000 MAX: 9998 UNITS: millimeters
      output.appendAll("prechrs" + ": "+ prechrs +"\n")
      output.appendAll("precdep" + ": "+ precdep +"\n")
  }
  empty;
}


