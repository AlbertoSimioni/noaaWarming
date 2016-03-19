/* SimpleApp.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.io.Source
import java.io.{ IOException, FileOutputStream, FileInputStream, File }
import java.util.zip.{ ZipEntry, ZipInputStream }
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.StringBuilder

import scala.collection.immutable.ListMap

object WeatherApp {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Global Warming Analyzer")
    //conf.set("spark.shuffle.consolidateFiles","true")
    val sc = new SparkContext(conf)
    val year = 1901
    for(year <- 1949 to 2016){
      val wheatherFile = "hdfs://hathi-surfsara:8020/user/lsde06/"+year+"small/*.gz"
      val lines = sc.textFile(wheatherFile)

      //PARSING
      val data = lines.map(Parser.parseLine)

      //FILTERING
      val filtered=data.filter(Temperature.filterTemp)


      //INDEXING BY REGION
      var region_data=filtered.map(Region.groupByRegion).cache()
      /*var sample = region_data.takeSample(false,300)
      var stringBuilder = new StringBuilder()
      for(a <- sample){
        stringBuilder.append(a.toString)
      }
      val output = scala.tools.nsc.io.File("/home/lsde06/sample1940.csv");
      output.writeAll(stringBuilder.toString);
      */

      //val region_data_sorted = region_data.sortBy(x => (x._1._1,x._1._2,x._2.temperature)).cache()


      //AVG TEMP PER REGION
      val region_sums=region_data.map(Temperature.stationDataToAvgResult).reduceByKey(Temperature.sumTempCounter)

      val region_avgs = region_sums.map{case(region,avgResult)=>(region, avgResult.temperature/avgResult.counter)}.cache()
      region_avgs.map(Temperature.avgResultToString).saveAsTextFile("/user/lsde06/results"+year+"/avgs/")

      //WORLD AVARAGE TEMPERATURE
      val region_count=region_avgs.count()
      val world_avg = region_avgs.map{case(region,avgTemp) => avgTemp}.reduce((avgTemp1,avgTemp2)=>avgTemp1+avgTemp2) / region_count
      val output = scala.tools.nsc.io.File("/home/lsde06/global/"+year+".json");
      output.writeAll("{ \"avg\":"+ world_avg.toString + ",");
      region_avgs.unpersist()


      //MIN AND MAX TEMPERATURE PER REGION
      val minMaxTemperature = region_data.map(Temperature.stationDataTominMaxResult).reduceByKey(Temperature.findMinMaxTemp).cache()
      minMaxTemperature.map(Temperature.minMaxResultToString).saveAsTextFile("/user/lsde06/results"+year+"/minmax/")

      //GLOBAL MIN AND MAX TEMPERATURE
      val world_minMax = minMaxTemperature.map{case(region,minMaxTemp) => minMaxTemp}.reduce(Temperature.findMinMaxTemp)
      output.appendAll("\"min\":" + world_minMax.minTemperature.toString+", \"max\":" + world_minMax.maxTemperature.toString+"}")
      minMaxTemperature.unpersist()


      //MEDIAN
      /*
       val elementsPerKey = region_data.countByKey()
       val elementsPerKeyOrdered = elementsPerKey.toSeq.sortBy(_._1)

       val region_data_indexed = region_data.zipWithIndex().map{
        case (v, idx) => (idx, v)
       }

       val region_data_indexed_cached = region_data_indexed.cache()
       var curIndex : Long = 0
       var mediansBuffer = new ListBuffer[Tuple2[Tuple2[Double,Double],StationData]]()
       for ((key,value) <- elementsPerKeyOrdered){
           // mediansBuffer += region_data_indexed_cached.lookup(curIndex  + value/2 ).head
          curIndex += value
        }
       val mediansList = mediansBuffer.toList
       val stringBuffer = new StringBuilder()
       for (median <- mediansList){
          stringBuffer.append(median._1._1 + "," + median._1._2 + "," + median._2.temperature +"\n")
       }

       val output2 = scala.tools.nsc.io.File("/home/lsde06/medians2010.csv");
       output2.writeAll(stringBuffer.toString);
      */
     region_data.unpersist()
   }
  }
}





