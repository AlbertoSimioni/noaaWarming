object Parser{

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

      if (temp != "9999") {
        tempD = ((temp.toDouble) / 10.0)
      }
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

       new StationData(id, date, latD, longD, tempD)

    } else {
       null
    }
  }
}

class StationData(
  val id: String,
  val date: java.util.Date,
  val latitude: Double,
  val longitude: Double,
  val temperature: Double
) extends Serializable {
  override def toString(): String = id + "," + date.toString + "," + latitude + "," + longitude + "," + temperature
}



object dataFields {
  val idPos = List(5, 10)
  val datePos = List(16, 23)
  val gmtPos = List(24, 27)
  val latPos = List(29, 34)
  val longPos = List(35, 41)
  val tempPos = List(88, 92)
}