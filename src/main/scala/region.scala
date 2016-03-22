object Region{
    val LATITUDE_SIZE = 1.0
    val LONGITUDE_SIZE = 1.0

    def groupByRegion(data: StationData):  Tuple2[Tuple2[Double,Double],Double] = {
        var regionLat = 0.0
        var regionLong = 0.0
        if(data.latitude >= 0){
            regionLat =((data.latitude/LATITUDE_SIZE).toInt) * LATITUDE_SIZE + LATITUDE_SIZE /2
        }
        else
            regionLat = ((data.latitude/LATITUDE_SIZE).toInt - 1) * LATITUDE_SIZE + LATITUDE_SIZE /2
        if(data.longitude >= 0)
            regionLong = ((data.longitude/LONGITUDE_SIZE).toInt) * LONGITUDE_SIZE + LONGITUDE_SIZE /2
        else
            regionLong = ((data.longitude/LONGITUDE_SIZE).toInt -1) * LONGITUDE_SIZE + LONGITUDE_SIZE /2
        ((regionLat,regionLong),data.temperature)
    }
}


/*
class Region(
    var regionLatitude: Double,
    var regionLongitude: Double) extends Serializable {
    override def toString(): String = regionLatitude + "," + regionLongitude
    /*def equals(that: Region): Boolean = {
        if((this.regionLongitude == that.regionLongitude) && (this.regionLatitude == that.regionLatitude))
            true
        else false
    }*/
    val key : String = regionLatitude.toString + regionLongitude.toString
}*/