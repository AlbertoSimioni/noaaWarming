def avgTemp(line: String): Double = {
  val tmp = line.substring(88 - 1, 92).trim
  if (tmp == "9999") {
    Double.MaxValue
  } else {
    ((tmp.toInt) / 10.0)
  }
}

def filterMonth(month: Int): Boolean = {
  if (line.substring(20, 21) == month)
    true
  else
    false
}

/**
 * Parsing
 */
def parseLine(line: String): String = {
  case class Field(
    val start: Int,
    val end: Int,
    val length: Int,
    val name: String,
    val description: String)

  val fields = Seq(
    //Field(1, 4, 4, "var_length", "Length"),
    Field(5, 10, 6, "usaf_id", "Id"),
    //Field(11, 15, 5, "wban", "Wban"), //Another identifier of the station
    Field(16, 23, 8, "date", "Date"), //YYYYMMDD format
    Field(24, 27, 4, "gmt", "GMT"), //HHMM   HH:[0,23]
    //Field(28, 28, 1, "data source", "Data Source"), //The type of the data source
    Field(29, 34, 6, "lat", "Lat"), //MIN: -90000 MAX: +90000  UNITS: Angular Degrees
    Field(35, 41, 7, "long", "Longitude"), //MIN: -179999 MAX: +180000 UNITS: Angular Degrees
    //Field(42, 46, 5, "report Type", "Report Type"),
    Field(47, 51, 5, "elev", "Elevation"), //MIN: -0400 MAX: +8850
    //Field(52, 56, 5, "call letters", ""),
    //Field(57, 60, 4, "qc_level", "Quality control"), //quality control process name
    //Field(61, 63, 3, "wind_dir", "Wind Direction"), //MIN: 001 MAX: 360 UNITS: starting from true north
    //Field(64, 64, 1, "wind dir flag", "Wind Direction Flag"), //direction quality code
    //Field(65, 65, 1, "wind_type", "Wind Type"), //Descriptor of the wind
    //Field(66, 69, 4, "wind_speed", "Wind Speed"), //MIN: 0000 MAX: 0900 UNITS: meters per second
    //Field(70, 70, 1, "wind_speed_flag", "Wind Speed Flag"), //quality
    //Field(71, 75, 5, "sky_ceiling", "Sky Ceiling"),//MIN: 00000 MAX: 22000 UNITS: Meters
    //Field(76, 76, 1, "sky_ceiling_flag", "Sky Ceiling Flag"), //quality
    //Field(77, 77, 1, "sky_ceiling_determ", "Sky Ceiling Determ"),//how was determined the ceiling
    //Field(78, 78, 1, "cavok_code", "Sky Condition Observation CAVOK code"),
    //Field(79, 84, 6, "visibility", "Visibility"), //MIN: 000000 MAX: 160000 UNITS: Meters
    //Field(85, 85, 1, "visibility_flag", "Visibility Flag"),//quality
    //Field(86, 86, 1, "visibility_var", "Visibility Var"), //if the visibility is variable
    //Field(87, 87, 1, "visibility_var_flag", "Visibility Var Flag"), //quality
    Field(88, 92, 5, "air_temp", "Temperature") //MIN: -0932 MAX: +0618 UNITS: Degrees Celsius
    //Field(93, 93, 1, "air_temp_flag", "Temperature Flag"),//quality
    //Field(94, 98, 5, "dew_point", "Dew Point"), //MIN: -0982 MAX: +0368 UNITS: Degrees Celsius
    //Field(99, 99, 1, "dew_point_flag", "Dew Point Flag"), //qaulity
    //Field(100, 104, 5, "sea_lev", "Sea Level Press"), //MIN: 08600 MAX: 10900 UNITS: Hectopascals
    //Field(105, 105, 1, "sea_lev_flag", "Sea Level Flag") //qaulity
    )
  val valueMap = scala.collection.mutable.Map[String, String]();

  if (line.length >= 61) {
    fields.foldLeft(valueMap) { (m, f) =>
      var fieldValue = line.substring(f.start - 1, f.end).trim
      //println(f.name);
      //println(fieldValue);
      m += (f.name -> fieldValue)
      //println(m.toString);
      //println(m(f.name))
      m
    }

    if (valueMap("air_temp") == "9999") {
      valueMap("air_temp") = ""
    } else {
      valueMap("air_temp") = ((valueMap("air_temp").toInt) / 10.0).toString
    }

    valueMap("lat") = ((valueMap("lat").toInt) / 1000.0).toString

    valueMap("long") = ((valueMap("long").toInt) / 1000.0).toString

    var additionalFields = line.substring(108)
    val aa1pos = additionalFields.indexOf("AA1", 0)
    var prechrs = ""
    var precdep = ""
    if (aa1pos != -1 && line.length >= aa1pos+9 ) {
      prechrs = additionalFields.substring(aa1pos + 3, aa1pos + 5)
      if (prechrs == "99") {
        prechrs = ""
      }
      precdep = (additionalFields.substring(aa1pos + 5, aa1pos + 9))
      if (precdep != "9999") {
        precdep = (precdep.toInt / 10.0).toString
      } else precdep = "";

      //liquid precipitations
      //output.appendAll(("prechrs" + ": "+ prechrs +"\n")
      //output.appendAll("precdep" + ": "+ precdep +"\n")
    }
    valueMap += ("prechrs" -> prechrs)
    valueMap += ("precdep" -> precdep)
  }
  }
  valueMap.values.mkString(",")
  //valueMap
}
//takes a file or folder with noaa data and parse them with the input function given
def parseRawData(file:String, parseFunction:String=>String)={
    val lines = sc.textFile(file)
    val parse = lines.map(parseFunction)
    parse
}

for (year <- 1901 to 1930) {
  val wheatherFile = "hdfs://hathi-surfsara:8020/user/lsde06/" + year.toString

  val parsed = parseRawData(wheatherFile, parseLine)


  val temps = lines.map(avgTemp)

  val avgYear = temps.reduce((a, b) => a + b) / temps.count()

  val parse = lines.map(parseLine)
  parse.saveAsTextFile("./parsedLines")
}
