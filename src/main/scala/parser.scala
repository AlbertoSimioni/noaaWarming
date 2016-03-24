object Parser{

  //Function that parse one line of the data, and returns an object with the fields that are used
  //for the anlysis
  def parseLine(line: String): (StationData) = {
    val formatDate = new java.text.SimpleDateFormat("yyyyMMdd")

    if (line.length >= 61) {

      //Station id
      var id = line.substring(dataFields.idPos(0) - 1, dataFields.idPos(1)).trim

      //Date
      var date = formatDate.parse(line.substring(dataFields.datePos(0) - 1, dataFields.datePos(1)).trim)

      //Latitude
      var lat = line.substring(dataFields.latPos(0) - 1, dataFields.latPos(1)).trim
      //normalized latitude
      var latD = ((lat.toInt) / 1000.0)

      //longitude
      var long = line.substring(dataFields.longPos(0) - 1, dataFields.longPos(1)).trim
      //normalized longitude
      var longD = ((long.toInt) / 1000.0)

      //temperature
      var temp = line.substring(dataFields.tempPos(0) - 1, dataFields.tempPos(1)).trim
      //normalized temperature
      var tempD = ((temp.toDouble) / 10.0)

      //Quality of the temperature value
      var qualityT = line.substring(dataFields.qualPos(0) - 1, dataFields.qualPos(1)).trim

      //Source of the data
      val reportType = line.substring(dataFields.repTypePos(0) - 1, dataFields.repTypePos(1)).trim

       new StationData(id, date, latD, longD, tempD, qualityT, reportType)

    } else {
       null
    }
  }
}

//Class that represent a record of data with the values that are used in the analysis
class StationData(
  val id: String,
  val date: java.util.Date,
  val latitude: Double,
  val longitude: Double,
  val temperature: Double,
  val tempQuality: String,
  val reportType: String
) extends Serializable {
  override def toString(): String = id + "," + date.toString + "," + latitude + "," + longitude + "," + temperature
}


//Position of the fields that are used for the analysis in a record of data formatted with the ISH format
object dataFields {
  val idPos = List(5, 10)
  val datePos = List(16, 23)
  val gmtPos = List(24, 27)
  val latPos = List(29, 34)
  val longPos = List(35, 41)
  val repTypePos = List(42,46)
  val tempPos = List(88, 92)
  val qualPos = List(93,93)
}