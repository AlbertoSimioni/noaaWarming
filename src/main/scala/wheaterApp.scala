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

/*
Main class where the analysis starts.
 */
object WeatherApp {

  //number of yearly data to group. The region results
  // will be computed on the data grouped on this number of years
  val NUM_GROUPED_YEARS = 5

  //main of the program. In this method are present all the steps of the computation
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Global Warming Analyzer")
    conf.set("spark.shuffle.consolidateFiles","true")
    val sc = new SparkContext(conf)

    //Variables that mantain the current grouped years that are being analyzed
    var startYear = 1901
    var endYear = startYear + NUM_GROUPED_YEARS -1

    //Variables that are used to group the yearly data.
    //The program computes some intermediate results for each year.
    //those results are grouped inside those two RDDs
    var fiveYearsAvgDataset :  RDD[((Double,Double),Double,Double)] = null //avg and std deviation
    var fiveYearsMinMaxDataset :  RDD[((Double,Double),MinMaxResult)] = null //min and max

    //The program performs the anlysis iteratively starting from the year 1901
    for(year <- 1901 to 2015){

      //location of the yearly data
      val wheatherFile = "hdfs://hathi-surfsara:8020/user/lsde06/"+year+"small/*.gz"
      val lines = sc.textFile(wheatherFile)

      //PARSING
      val data = lines.map(Parser.parseLine)

      //FILTERING
      val filtered=data.filter(Filter.filterLine)


      //INDEXING BY REGION
      //this RDD is cached because two different computation on it are performed. One to compute
      //the avg and the std dev, one to compute min and max
      var region_data=filtered.map(Region.groupByRegion).persist(StorageLevel.MEMORY_AND_DISK)


      //AVG TEMP AND STD DEV PER REGION PER SINGLE YEAR
      //1° It sums the values of the temperatures and the square of the temperatures
      val region_sums=region_data.map(Temperature.dataToAvgResult).reduceByKey(Temperature.sumTempCounter)
      //2° Divides the sums by number of elements that were summed
      val region_avgs = region_sums.map{case(region,avgResult)=>(region, avgResult.temperatureSum/avgResult.counter, avgResult.temperatureSumsq/avgResult.counter)}.cache()

      //MIN AND MAX TEMPERATURE PER REGION PER SINGLE YEAR
      val minMaxTemperature = region_data.map(Temperature.dataTominMaxResult).reduceByKey(Temperature.findMinMaxTemp).cache()

      //Removing from cache the data indexed by region. From now on, only the intermediate results
      //will be used for the next computations. Those intermediate results have been cached too
      region_data.unpersist()

      //USA AVARAGE STDDEV MIN MAX TEMPERATURE
      calculateAvgMinMaxByCoordinates(region_avgs,minMaxTemperature, new RegionCoordinates(50,-128,24,-65), year,"usa")

      //EUROPE AVARAGE STDDEV MIN MAX TEMPERATURE
      calculateAvgMinMaxByCoordinates(region_avgs,minMaxTemperature, new RegionCoordinates(71,-11,35,41), year,"europe")

      //AUSTRALIA AVARAGE MIN MAX TEMPERATURE
      calculateAvgMinMaxByCoordinates(region_avgs,minMaxTemperature, new RegionCoordinates(-9,111,-44,156),year ,"australia")


      //GROUPING FIVE YEARS DATA
      if(year == startYear){
        fiveYearsAvgDataset = region_avgs
        fiveYearsMinMaxDataset = minMaxTemperature
      }
      else{
        val oldAvg = fiveYearsAvgDataset
        val oldMinMax = fiveYearsMinMaxDataset
        //here the intermediate yearly results are joined for the five years analysis
        fiveYearsAvgDataset = fiveYearsAvgDataset.union(region_avgs)
        fiveYearsMinMaxDataset = fiveYearsMinMaxDataset.union(minMaxTemperature)
        if(year == startYear+1){
         oldAvg.unpersist()
         oldMinMax.unpersist()
       }
      }

      if(year == endYear){ //if true we have reached the fifth year
        //COMPUTING AVG, STDDEV ON THE FIVE YEAR DATA
        val years_sum = fiveYearsAvgDataset.map{case(pos,avg,sumsq) => (pos,new AvgResult(1,avg,sumsq))}.reduceByKey(Temperature.sumTempCounter)
        val region_five_years_avgs = years_sum.map(Temperature.computeAvgStd)
        //Storing avg and stddev results
        region_five_years_avgs.map(Temperature.avgResultToString).saveAsTextFile("/user/lsde06/results"+startYear+"_"+endYear+"/avgs/")

        //COMPUTING MIN, MAX ON THE FIVE YEAR DATA
        val five_years_minMaxTemperature = fiveYearsMinMaxDataset.reduceByKey(Temperature.findMinMaxTemp)
        five_years_minMaxTemperature.map(Temperature.minMaxResultToString).saveAsTextFile("/user/lsde06/results"+startYear+"_"+endYear+"/minmax/")

        //updating the start year and end year of the next interval
        startYear = endYear + 1
        endYear += NUM_GROUPED_YEARS
        region_avgs.unpersist()
        minMaxTemperature.unpersist()
      }


   } // for
  } // main

  /*Computes min, max, avg, stddev in the rect of coordinates given in input
  This function is used to compute the yearly results of USA, Europe and Australia

  avg_data: intermediate results of avg and stddev per each region
  minMax_data: intermediate results of min and max per each region
  coordinates: rect of coordinates where to compute the results
  year: number of the current year analyzed
  regionName: name of the region that will be analyzed
  */
  def calculateAvgMinMaxByCoordinates(avg_data: RDD[((Double,Double),Double,Double)], minMax_data:RDD[((Double,Double),MinMaxResult)], coordinates: RegionCoordinates, year: Int , regionName: String){

      //Filtering the data that are not inside the rect of coordinates
      val avg_filtered = avg_data.filter(Filter.filterCoordinates2(coordinates))
      val data_count= avg_filtered.count()
      //if there are still some data after the filtering
      if (data_count!= 0){
        //computing avg and stddev
        val temp_sum = avg_filtered.map{case(region,avgTemp,sumsq) => new AvgResult(1,avgTemp,sumsq)}.reduce(Temperature.sumTempCounter)
        val avgStd = Temperature.computeAvgStd(((0,0),temp_sum))

        //computing min and max
        val minMax_filtered = minMax_data.filter(Filter.filterCoordinates(coordinates))
        val minMax = minMax_filtered.map{case(region,minMaxTemp) => minMaxTemp}.reduce(Temperature.findMinMaxTemp)

        //storing the results
        val output = new FileWriter(new File("/home/lsde06/"+regionName+".csv"),true)
        output.write(year +"," + avgStd._2 + "," +avgStd._3 +"," + minMax.minTemperature + "," +minMax.maxTemperature+","+data_count+"\n")
        output.close()
    }
  }
} // wheatherApp




