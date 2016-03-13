
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

 def maxTemp(x: StationData, y: StationData): StationData = {
    if (x.temperature >= y.temperature)
      x
    else
      y
  }

  def minTemp(x: StationData, y: StationData): StationData = {
    if (x.temperature > y.temperature)
      y
    else
      x
  }

def filterTemp(s: StationData): Boolean = {
  if (s.temperature == 999.9)
    false
  else
    true
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



def extractTemp(d: (String, StationData)): (String, Double) = {
  (d(0), d(1).temperature)
}

val wheatherFile = "hdfs://hathi-surfsara:8020/user/lsde06/2015"
val lines = sc.textFile(wheatherFile)

val data = lines.map(parseLine)

//index data by id
val indexed = data.map(data => (data.id, data))


//remove not valid temperature and index the data
val filtered=data.filter(filterTemp)
val indexFilteredByTemp=filtered.map(data => (data.id, data))


// avgs
val sums=filtered.map(c=>(1,c.temperature)).reduce((v1,v2)=>(v1._1+v2._1, v1._2+v2._2))
sums._2/sums._1

val LATITUDE_SIZE = 15
val LONGITUDE_SIZE = 15
//index data by region
var region_data=filtered.map(data => (((data.latitude/LATITUDE_SIZE).toInt,(data.longitude/LONGITUDE_SIZE).toInt),data ))

val region_sums=region_data.map{ case (pos,data) =>  (pos, (1,data.temperature)) }.reduceByKey{ case ((c1, s1), (c2, s2)) => (c1 + c2, s1 + s2) }

val region_avgs = region_sums.map{case(pos,(num,sum))=>(pos, sum/num)}

val region_count=region_avgs.count()
val world_avg = region_avgs.map( data => data._2).reduce((a,b)=>a+b) / region_count

//find max temperatures
val maxTemperature = indexed.reduceByKey(maxTemp)

// isolate temperatures 
val temps = indexFilteredByTemp.map { case (k, v) => (k, (1, v.temperature)) }
val avgTemps = temps.reduceByKey { case ((c1, s1), (c2, s2)) => (c1 + c2, s1 + s2) }.map { case (k, (count, sum)) => (k, sum / count) }



// For the median
 val sorted = filtered.sortBy( c=>c.temperature).zipWithIndex().map {
    case (v, idx) => (idx, v)
  }

  val count = sorted.count()

  val median: Double = if (count % 2 == 0) {
    val l = count / 2 - 1
    val r = l + 1
    (sorted.lookup(l).head.temperature + sorted.lookup(r).head.temperature).toDouble / 2
  } else sorted.lookup(count / 2).head.temperature.toDouble
