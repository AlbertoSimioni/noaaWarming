//contains some functions that are used to analyze the temperature results
object Temperature{

    //Converts avg and stddev result to string, useful to write in csv the results
    def avgResultToString(x: ((Double,Double),Double,Double)): String = {
            x._1._1+ "," + x._1._2 + "," + x._2+ "," +x._3
        }
    //Converts min and max result to string, useful to write in csv the results
    def minMaxResultToString(x: ((Double,Double),MinMaxResult)): String = {
            x._1._1+ "," + x._1._2 + "," + x._2
        }

    //Creates a new object with the fields necessary to compute avg and stddev inside a reducebykey
    def dataToAvgResult(x: ((Double,Double),Double)):((Double,Double),AvgResult) = {
        (x._1, new AvgResult(1,x._2, x._2 * x._2))
        }

    //Sums the value of the temperature and the square of the temperature of the two given input
    def sumTempCounter(x: AvgResult, y: AvgResult): AvgResult = {
        new AvgResult(x.counter + y.counter, x.temperatureSum + y.temperatureSum, x.temperatureSumsq + y.temperatureSumsq)
    }

    //Returns the value of the avg and stddev by giving in input the sum of the temperatures and the
    //sum of the squares of the temperatures
    def computeAvgStd(x :((Double,Double),AvgResult)) : ((Double,Double),Double,Double) = {
        val avg = x._2.temperatureSum/x._2.counter
        val variance = x._2.temperatureSumsq/x._2.counter - (avg * avg)
        val stdDev = scala.math.sqrt(variance)
        (x._1,avg,stdDev)
    }

    //returns the minimun and the maximum temperature of the two inputs
    def findMinMaxTemp(x: MinMaxResult, y: MinMaxResult):  MinMaxResult = {
        val result = new MinMaxResult(0.0,0.0)

        if (x.minTemperature >= y.minTemperature)
            result.minTemperature = y.minTemperature
        else
            result.minTemperature = x.minTemperature

        if (x.maxTemperature >= y.maxTemperature)
            result.maxTemperature = x.maxTemperature
        else
            result.maxTemperature = y.maxTemperature

        result
    }

    //Creates a new object with the fields necessary to compute min and max inside a reducebykey
    def dataTominMaxResult(x: ((Double,Double),Double)):
         Tuple2[Tuple2[Double,Double],MinMaxResult] = {
        val result = new MinMaxResult(0.0,0.0)
        result.minTemperature = x._2
        result.maxTemperature = x._2
        (x._1,result)
    }

}

//class that is used during the computation of the avg and of the stddev
class AvgResult(
    var counter : Int,
    var temperatureSum : Double,
    var temperatureSumsq: Double
    ) extends Serializable {
    override def toString(): String = counter + "," + temperatureSum +temperatureSumsq
}

//class that is used during the computation of the minimun and maximun temperature
class MinMaxResult(
    var minTemperature: Double,
    var maxTemperature: Double) extends Serializable {
    override def toString(): String = minTemperature + "," + maxTemperature
}
