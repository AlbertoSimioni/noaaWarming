object Filter{

  def filterLine(s: StationData): Boolean = {
     if(List("0","1", "4","5", "9","M", "A","C","P","R","U" ) contains s.tempQuality){
       if (s.temperature < 62.0 && s.temperature > -94.0){
         if(s.reportType != "FM-13" && s.reportType != "FM-14")
             true
         else false
       }
       else false
      }
      else false
  }

  def filterCoordinates(coordinates : RegionCoordinates)(regionAvg: ((Double,Double),Any)): Boolean = {
    if((coordinates.bottom <= regionAvg._1._1 && regionAvg._1._1 <= coordinates.top) && (coordinates.left <= regionAvg._1._2 && regionAvg._1._2 <= coordinates.right))
      true
    else
      false
  }

  def filterCoordinates2(coordinates : RegionCoordinates)(regionAvg: ((Double,Double),Any,Any)): Boolean = {
    filterCoordinates(coordinates)((regionAvg._1),regionAvg._2)
  }

}


class RegionCoordinates(
  var top: Double,
  var left: Double,
  var bottom: Double,
  var right: Double
  )  extends Serializable {}