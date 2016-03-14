/* SimpleApp.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.io.Source
import java.io.{ IOException, FileOutputStream, FileInputStream, File }
import java.util.zip.{ ZipEntry, ZipInputStream }

import ru.retailrocket.spark.multitool.Loaders._



object WeatherApp {

  def main(args: Array[String]) {

    val wheatherFile = "hdfs://hathi-surfsara:8020/user/lsde06/1940/*.gz"
    val conf = new SparkConf().setAppName("Global Warming Analyzer")
    val sc = new SparkContext(conf)
    val lines = sc.combineTextFile(wheatherFile)

    //PARSING
    val data = lines.map(Parser.parseLine)

    //FILTERING
    val filtered=data.filter(tempFunctions.filterTemp)


    //INDEXING BY REGION
    var region_data=filtered.map(Region.groupByRegion).cache()

    //AVG TEMP PER REGION
    val region_sums=region_data.map(Temperature.stationDataToAvgResult).reduceByKey(Temperature.sumTempCounter)

    val region_avgs = region_sums.map{case(pos,avgResult)=>(pos, avgResult.counter/avgResult.temperature)}.cache()
    region_avgs.map(Temperature.avgResultToString).saveAsTextFile("/user/lsde06/results1940/avgs/")

    //WORLD AVARAGE TEMPERATURE
    val region_count=region_avgs.count()
    val world_avg = region_avgs.map( data => data._2).reduce((a,b)=>a+b) / region_count
    val output = scala.tools.nsc.io.File("/home/lsde06/global1940.csv");
    output.writeAll(world_avg.toString);

    //ragruppare max, min e avg in un unica iterazione invece di farla separate
    val minMaxTemperature = region_data.map(Temperature.stationDataTominMaxResult).reduceByKey(Temperature.minMaxTemp)
    minMaxTemperature.map(Temperature.minMaxResultToString).saveAsTextFile("/user/lsde06/results1940/minmax/")


    /*
    // MEDIAN COMPUTAT
    ION -> METTERE APPOSTO PE REGIONE
    val sorted = filtered.sortBy( c=>c.temperature).zipWithIndex().map {
        case (v, idx) => (idx, v)
      }
    val count = sorted.count()
    val median: Double = if (count % 2 == 0) {
        val l = count / 2 - 1
        val r = l + 1
        (sorted.lookup(l).head.temperature + sorted.lookup(r).head.temperature).toDouble / 2
      } else sorted.lookup(count / 2).head.temperature.toDouble
  }*/
  }
}

object tempFunctions {

  def filterTemp(s: StationData): Boolean = {
    if (s.temperature == 999.9)
      false
    else
      true
  }
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


