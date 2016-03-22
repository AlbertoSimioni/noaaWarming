object Temperature{
    def avgResultToString(x: Tuple2[Tuple2[Double,Double],Double]): String = {
            x._1._1+ "," + x._1._2 + "," + x._2
        }

    def minMaxResultToString(x: Tuple2[Tuple2[Double,Double],MinMaxResult]): String = {
            x._1._1+ "," + x._1._2 + "," + x._2
        }


    def dataToAvgResult(x: Tuple2[Tuple2[Double,Double],Double]):
        Tuple2[Tuple2[Double,Double],AvgResult] = {
        (x._1, new AvgResult(1,x._2))
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


    def dataTominMaxResult(x: Tuple2[Tuple2[Double,Double],Double]):
         Tuple2[Tuple2[Double,Double],MinMaxResult] = {
        val result = new MinMaxResult(0.0,0.0)
        result.minTemperature = x._2
        result.maxTemperature = x._2
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
