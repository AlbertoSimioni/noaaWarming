

object dataFields {
  val idPos = List(5, 10)
  val datePos = List(16, 23)
  val gmtPos = List(24, 27)
  val latPos = List(29, 34)
  val longPos = List(35, 41)
  val tempPos = List(88, 92)
  val tempQualityPos = List(93, 93)
}

class StationData(
  val id: String,
  val date: java.util.Date,
  val latitude: Double,
  val longitude: Double,
  val temperature: Double,
  val tempQuality: String,
  val prechrs: Int,
  val precdep: Double) extends Serializable {
  override def toString(): String = id + "," + date.toString + "," + latitude + "," + longitude + "," + temperature + "," + prechrs + "," + precdep
}

def parseLine(line: String): (StationData) = {
  val formatDate = new java.text.SimpleDateFormat("yyyyMMdd")

  var res: String = ""

  if (line.length >= 61) {

    var id = line.substring(dataFields.idPos(0) - 1, dataFields.idPos(1)).trim

    var date = formatDate.parse(line.substring(dataFields.datePos(0) - 1, dataFields.datePos(1)).trim)

    var lat = line.substring(dataFields.latPos(0) - 1, dataFields.latPos(1)).trim
    var latD = ((lat.toInt) / 1000.0)

    var long = line.substring(dataFields.longPos(0) - 1, dataFields.longPos(1)).trim
    var longD = ((long.toInt) / 1000.0)

    var temp = line.substring(dataFields.tempPos(0) - 1, dataFields.tempPos(1)).trim
    var tempD = 999.9
    var tempQ = line.substring(dataFields.tempQualityPos(0) - 1, dataFields.tempQualityPos(1)).trim
    if (temp != "9999") {
      tempD = ((temp.toDouble) / 10.0)
    }

    var prechrs = 1
    var precdep = -0.1
    /*
      if (line.length > 108) {
        var additionalFields = line.substring(108)
        val aa1pos = additionalFields.indexOf("AA1", 0)

        if (aa1pos != -1 && line.length >= aa1pos + 9) {
          prechrs = additionalFields.substring(aa1pos + 3, aa1pos + 5).toInt
          if (prechrs == 99) {
            prechrs = -1
          }
          precdep = (additionalFields.substring(aa1pos + 5, aa1pos + 9)).toDouble
          if (precdep != 9999.toDouble) {
            precdep = (precdep.toInt / 10.0)
          } else precdep = -0.1;

          //liquid precipitations
          //output.appendAll(("prechrs" + ": "+ prechrs +"\n")
          //output.appendAll("precdep" + ": "+ precdep +"\n")
        }
      }
      */

    new StationData(id, date, latD, longD, tempD, tempQ, prechrs, precdep)

  } else {
    null
  }
}

def filterTemp(s: StationData): Boolean = {
  if (List("0", "1", "4", "5", "9", "M", "A", "C", "P", "R", "U") contains s.tempQuality) {
    if (s.temperature < 62.0 && s.temperature > -94.0)
      true
    else
      false
  } else { false }

}

val years=List(1907, 1930, 1945, 1962, 1968, 1972, 2014)

for(var year <- years) {


val wheatherFile = "hdfs://hathi-surfsara:8020/user/lsde06/"+year.toString+"small/*.gz"
val lines = sc.textFile(wheatherFile)

val data = lines.map(parseLine)
val pos = data.map(d => (d.id, (d.latitude, d.longitude)))
val pos2 = pos.reduceByKey((a, b) => a).map(data => data._2)
val csv = pos2.map(data => data._1 + "," + data._2)
csv.saveAsTextFile("/user/lsde06/"+year.toString+"stations/") 
}
