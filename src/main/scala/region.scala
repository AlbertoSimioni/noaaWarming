object Region{
    val LATITUDE_SIZE = 1.0
    val LONGITUDE_SIZE = 1.0

    def groupByRegion(data: StationData):  ((Double,Double),Double) = {
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
