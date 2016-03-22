/* SimpleApp.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.io.Source
import java.io.{ IOException, FileOutputStream, FileInputStream, File }
import org.apache.spark.storage._
import scala.collection.immutable.ListMap
import java.io._
import org.apache.spark.rdd.RDD
object WeatherApp {

  val NUM_GROUPED_YEARS = 5

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Global Warming Analyzer")
    conf.set("spark.shuffle.consolidateFiles","true")
    val sc = new SparkContext(conf)

    var startYear = 1901
    var endYear = startYear + NUM_GROUPED_YEARS -1

    var fiveYearsAvgDataset :  RDD[((Double,Double),Double,Double)] = null
    var fiveYearsMinMaxDataset :  RDD[((Double,Double),MinMaxResult)] = null
    for(year <- 1901 to 2015){
      val wheatherFile = "hdfs://hathi-surfsara:8020/user/lsde06/"+year+"small/*.gz"
      val lines = sc.textFile(wheatherFile)

      //PARSING
      val data = lines.map(Parser.parseLine)

      //FILTERING
      val filtered=data.filter(Filter.filterLine)


      //INDEXING BY REGION
      var region_data=filtered.map(Region.groupByRegion).persist(StorageLevel.MEMORY_AND_DISK)


      //AVG TEMP PER REGION
      val region_sums=region_data.map(Temperature.dataToAvgResult).reduceByKey(Temperature.sumTempCounter)

      val region_avgs = region_sums.map{case(region,avgResult)=>(region, avgResult.temperatureSum/avgResult.counter, avgResult.temperatureSumsq/avgResult.counter)}.cache()

      //MIN AND MAX TEMPERATURE PER REGION
      val minMaxTemperature = region_data.map(Temperature.dataTominMaxResult).reduceByKey(Temperature.findMinMaxTemp).cache()

      region_data.unpersist()

      //USA AVARAGE MIN MAX TEMPERATURE
      calculateAvgMinMaxByCoordinates(region_avgs,minMaxTemperature, new RegionCoordinates(50,-128,24,-65), year,"usa")

      //EUROPE AVARAGE MIN MAX TEMPERATURE
      calculateAvgMinMaxByCoordinates(region_avgs,minMaxTemperature, new RegionCoordinates(71,-11,35,41), year,"europe")

      //AUSTRALIA AVARAGE MIN MAX TEMPERATURE
      calculateAvgMinMaxByCoordinates(region_avgs,minMaxTemperature, new RegionCoordinates(-9,111,-44,156),year ,"australia")


      //GROUPING DIFFERENT YEAR DATA
      if(year == startYear){
        fiveYearsAvgDataset = region_avgs
        //fiveYearsMinMaxDataset = minMaxTemperature
      }

      else{
        val oldAvg = fiveYearsAvgDataset
        val oldMinMax = fiveYearsMinMaxDataset
        fiveYearsAvgDataset = fiveYearsAvgDataset.union(region_avgs)
        fiveYearsMinMaxDataset = fiveYearsMinMaxDataset.union(minMaxTemperature)
        if(year == startYear+1){
         oldAvg.unpersist()
         oldMinMax.unpersist()
       }
      }

      if(year == endYear){
        //summing the value of the different years
        val years_sum = fiveYearsAvgDataset.map{case(pos,avg,sumsq) => (pos,new AvgResult(1,avg,sumsq))}.reduceByKey(Temperature.sumTempCounter)
        //calculating avgs
        val region_five_years_avgs = years_sum.map(Temperature.computeAvgStd)
        region_five_years_avgs.map(Temperature.avgResultToString).saveAsTextFile("/user/lsde06/results"+startYear+"_"+endYear+"/avgs/")

        val five_years_minMaxTemperature = fiveYearsMinMaxDataset.reduceByKey(Temperature.findMinMaxTemp)
        five_years_minMaxTemperature.map(Temperature.minMaxResultToString).saveAsTextFile("/user/lsde06/results"+startYear+"_"+endYear+"/minmax/")

        startYear = endYear + 1
        endYear += NUM_GROUPED_YEARS
        region_avgs.unpersist()
        minMaxTemperature.unpersist()
      }


   } // for
  } // main


  def calculateAvgMinMaxByCoordinates(avg_data: RDD[((Double,Double),Double,Double)], minMax_data:RDD[((Double,Double),MinMaxResult)], coordinates: RegionCoordinates, year: Int , regionName: String){

      val avg_filtered = avg_data.filter(Filter.filterCoordinates2(coordinates))
      val data_count= avg_filtered.count()
      if (data_count!= 0){
        val temp_sum = avg_filtered.map{case(region,avgTemp,sumsq) => new AvgResult(1,avgTemp,sumsq)}.reduce(Temperature.sumTempCounter)

        val avgStd = Temperature.computeAvgStd(((0,0),temp_sum))

        val minMax_filtered = minMax_data.filter(Filter.filterCoordinates(coordinates))
        val minMax = minMax_filtered.map{case(region,minMaxTemp) => minMaxTemp}.reduce(Temperature.findMinMaxTemp)
        val output = new FileWriter(new File("/home/lsde06/"+regionName+".csv"),true)
        output.write(year +"," + avgStd._2 + "," +avgStd._3 +"," + minMax.minTemperature + "," +minMax.maxTemperature+","+data_count+"\n")
        output.close()
    }
  }
} // wheatherApp




