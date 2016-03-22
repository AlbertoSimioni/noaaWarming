object Temperature{
    def avgResultToString(x: ((Double,Double),Double,Double)): String = {
            x._1._1+ "," + x._1._2 + "," + x._2+ "," +x._3
        }

    def minMaxResultToString(x: ((Double,Double),MinMaxResult)): String = {
            x._1._1+ "," + x._1._2 + "," + x._2
        }


    def dataToAvgResult(x: ((Double,Double),Double)):
        Tuple2[Tuple2[Double,Double],AvgResult] = {
        (x._1, new AvgResult(1,x._2, x._2 * x._2))
        }

    def sumTempCounter(x: AvgResult, y: AvgResult): AvgResult = {
        new AvgResult(x.counter + y.counter, x.temperatureSum + y.temperatureSum, x.temperatureSumsq + y.temperatureSumsq)
    }

    def computeAvgStd(x :((Double,Double),AvgResult)) : ((Double,Double),Double,Double) = {
        val avg = x._2.temperatureSum/x._2.counter
        val variance = x._2.temperatureSumsq/x._2.counter - (avg * avg)
        val stdDev = scala.math.sqrt(variance)
        (x._1,avg,stdDev)
    }


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


    def dataTominMaxResult(x: ((Double,Double),Double)):
         Tuple2[Tuple2[Double,Double],MinMaxResult] = {
        val result = new MinMaxResult(0.0,0.0)
        result.minTemperature = x._2
        result.maxTemperature = x._2
        (x._1,result)
    }

}


class AvgResult(
    var counter : Int,
    var temperatureSum : Double,
    var temperatureSumsq: Double
    ) extends Serializable {
    override def toString(): String = counter + "," + temperatureSum +temperatureSumsq
}


class MinMaxResult(
    var minTemperature: Double,
    var maxTemperature: Double) extends Serializable {
    override def toString(): String = minTemperature + "," + maxTemperature
}
