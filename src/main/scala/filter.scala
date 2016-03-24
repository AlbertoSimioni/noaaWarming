object Filter{

  //Filters the data that are wrong or not usefult for the analysis
  def filterLine(s: StationData): Boolean = {
    //checks the quality attribute of the temperature, accepting only values that should be correct
     if(List("0","1", "4","5", "9","M", "A","C","P","R","U" ) contains s.tempQuality){
       //The value of the temperature shouldn't be outside of this range
       //this is an additional control, in case the quality attribute isn't sufficient
       if (s.temperature < 62.0 && s.temperature > -94.0){
        //discarding the values that are recorded by stations that is moving
         if(s.reportType != "FM-13" && s.reportType != "FM-14")
             true
         else false
       }
       else false
      }
      else false
  }

  //Filters the coordinates of the belonging region with the rect of coordinates given in input
  def filterCoordinates(coordinates : RegionCoordinates)(regionAvg: ((Double,Double),Any)): Boolean = {
    if((coordinates.bottom <= regionAvg._1._1 && regionAvg._1._1 <= coordinates.top) && (coordinates.left <= regionAvg._1._2 && regionAvg._1._2 <= coordinates.right))
      true
    else
      false
  }
  //Same as above
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