

object dataFields {
  val idPos = List(5, 10)
  val datePos = List(16, 23)
  val gmtPos = List(24, 27)
  val latPos = List(29, 34)
  val longPos = List(35, 41)
  val tempPos = List(88, 92)
}


val dataFieldsObj=new dataFields

class StationData (
                  val id: String,
                val   date: java.util.Date,
                val   latitude: Double,
                val   longitude: Double,
                val   temperature: Double
                  )extends Serializable{
  override def toString():String = id + "," + date.toString + "," +latitude + ","+longitude +"," + temperature
}


def parseLine(line: String): StationData = {
val formatDate = new java.text.SimpleDateFormat("yyyyMMdd")

  var res: String = ""

  if (line.length >= 61) {

    var id = line.substring(dataFieldsObj.idPos(0) - 1, dataFieldsObj.idPos(1)).trim

    var date=formatDate.parse(line.substring(dataFieldsObj.datePos(0) - 1, dataFieldsObj.datePos(1)).trim)

    var lat = line.substring(dataFieldsObj.latPos(0) - 1, dataFieldsObj.latPos(1)).trim
    var latD = ((lat.toInt) / 1000.0)

    var long = line.substring(dataFieldsObj.longPos(0) - 1, dataFieldsObj.longPos(1)).trim
    var longD = ((long.toInt) / 1000.0)

    var temp = line.substring(dataFieldsObj.tempPos(0) - 1, dataFieldsObj.tempPos(1)).trim
    var tempD = Double.MaxValue

    if (temp == "9999") {
      temp = ""
    } else {
      tempD = ((temp.toInt) / 10.0)
    }

    if(line.length >108){
    var additionalFields = line.substring(108)
    val aa1pos = additionalFields.indexOf("AA1", 0)
    var prechrs = ""
    var precdep = ""
    if (aa1pos != -1 && line.length >= aa1pos+9) {
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
  }

    new StationData(id, date, latD, longD, tempD)

  } else {
    null
  }
}

val wheatherFile = "hdfs://hathi-surfsara:8020/user/lsde06/1970"
    val lines = sc.textFile(wheatherFile)
    val data = lines.map(parseLine)
    val sample = data.sample(false, 0.2, 0)
    sample.take(1)