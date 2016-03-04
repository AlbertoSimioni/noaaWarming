/* SimpleApp.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.io.Source
import java.io.{ IOException, FileOutputStream, FileInputStream, File }
import java.util.zip.{ ZipEntry, ZipInputStream }


import ru.retailrocket.spark.multitool.Loaders._

object WeatherApp {
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

       new StationData(id, date, latD, longD, tempD, prechrs, precdep)

    } else {
       null
    }
  }



  def main(args: Array[String]) {

    val wheatherFile = "hdfs://hathi-surfsara:8020/user/lsde06/2010/*.gz"
    val conf = new SparkConf().setAppName("Global Warming Analyzer")
    val sc = new SparkContext(conf)
    val lines = sc.combineTextFile(wheatherFile)

    val data = lines.map(parseLine)

    //val indexed = data.map(data => (data.id, data))

    //remove not valid temperature and index the data
    val filtered=data.filter(tempFunctions.filterTemp)
    val indexFilteredByTemp=filtered.map(data => (data.id, data))
    //find max temperatures
    //val maxTemperature = indexed.reduceByKey(maxTemp)

    // isolate temperatures
    val temps = indexFilteredByTemp.map { case (k, v) => (k, (1, v.temperature,v.latitude,v.longitude)) }
    val avgTemps = temps.reduceByKey { case ((c1, s1,lat1,long1), (c2, s2,lat2,long2)) => (c1 + c2, s1 + s2,lat1,long1) }.map { case (k, (count, sum,lat,long)) => (k, sum / count,lat,long) }

    avgTemps.map(tempFunctions.resultToCSV2).saveAsTextFile("/user/lsde06/results2010/")
  }
}

object tempFunctions {

  def filterTemp(s: StationData): Boolean = {
    if (s.temperature == 999.9)
      false
    else
      true
  }

  def ResultToCSV(key: String, avg: Double,lat: Double, long: Double ): String ={
    key + "," + avg + "," + lat +"," + long
  }

  val resultToCSV2 = (ResultToCSV _).tupled

}

object dataFields {
  val idPos = List(5, 10)
  val datePos = List(16, 23)
  val gmtPos = List(24, 27)
  val latPos = List(29, 34)
  val longPos = List(35, 41)
  val tempPos = List(88, 92)
}


class StationData(
  val id: String,
  val date: java.util.Date,
  val latitude: Double,
  val longitude: Double,
  val temperature: Double,
  val prechrs: Int,
  val precdep: Double) extends Serializable {
  override def toString(): String = id + "," + date.toString + "," + latitude + "," + longitude + "," + temperature + "," + prechrs + "," + precdep
}


