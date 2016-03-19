object Temperature{
    def avgResultToString(x: Tuple2[Tuple2[Double,Double],Double]): String = {
            x._1._1+ "," + x._1._2 + "," + x._2
        }

    def minMaxResultToString(x: Tuple2[Tuple2[Double,Double],MinMaxResult]): String = {
            x._1._1+ "," + x._1._2 + "," + x._2
        }


    def stationDataToAvgResult(x: Tuple2[Tuple2[Double,Double],StationData]):
        Tuple2[Tuple2[Double,Double],AvgResult] = {
        (x._1, new AvgResult(1,x._2.temperature))
        }

    def sumTempCounter(x: AvgResult, y: AvgResult): AvgResult = {
        new AvgResult(x.counter + y.counter, x.temperature + y.temperature)
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

    def filterTemp(s: StationData): Boolean = {
        if(List("0","1", "4","5", "9","M", "A","C","P","R","U" ) contains s.tempQuality){
          if (s.temperature < 62.0 && s.temperature > -94.0)
            true
          else
            false
        }
        else
            false
    }


    def stationDataTominMaxResult(x: Tuple2[Tuple2[Double,Double],StationData]):
         Tuple2[Tuple2[Double,Double],MinMaxResult] = {
        val result = new MinMaxResult(0.0,0.0)
        result.minTemperature = x._2.temperature
        result.maxTemperature = x._2.temperature
        (x._1,result)
    }

}


class AvgResult(
    var counter : Int,
    var temperature : Double
    ) extends Serializable {
    override def toString(): String = counter + "," + temperature
}


class MinMaxResult(
    var minTemperature: Double,
    var maxTemperature: Double) extends Serializable {
    override def toString(): String = minTemperature + "," + maxTemperature
}
