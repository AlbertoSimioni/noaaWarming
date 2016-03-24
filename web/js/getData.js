 var heatmapIntensity = 10;

 function getData(year1, year2, metric) {
    console.log(metric)
    mapPos = {}
     testData = {
        max: heatmapIntensity,
        data: []
     }
     console.log("getting data");
     var folder = null;
     if(metric === "avgTemp" || metric === "stddev") folder = "avgs"
     if(metric === "minTemp" || metric === "maxTemp") folder = "minmax"
     loadJSON("results/"+folder+"/"+year2 + '.json',
         function (data) {
             fillData(data,mapPos,metric);
             loadJSON("results/"+folder+"/"+year1 + ".json",
                 function (data2) {
                     calculateDifference(data2,mapPos,testData,metric);
                     insertData(testData);

                     console.log("finish");

                 },
                 function (xhr) {
                     console.error(xhr);
                 }
             );
         },
         function (xhr) {
             console.error(xhr);
         }

     );
 }

 function fillData(data,mapPos,metric) {
     data.results.forEach(function (res) {

         mapPos[res.lat +"_" + res.long] = res[metric];
         //testData.data.push({lat: res.latitude, lng:res.longitude, count: res.avgTemp})
     });
 }

 function calculateDifference(data2,mapPos,testData,metric) {
     var min = 1000;
     var max = -1000;
     data2.results.forEach(function (res) {

         if (mapPos[res.lat +"_" + res.long] !== undefined) {
            //console.log(mapPos[res.lat +"_" + res.long])
             var diff = difference(mapPos[res.lat +"_" + res.long], res[metric]);
             if (diff > max) {

                 max = diff;
             }
             if (diff < min) {
                 min = diff;
             }
             //repeated for each region
             testData.data.push({
                 lat: res.lat,
                 lng: res.long,
                 temp: diff
             });
         }
     });

     min = Math.abs(min);
     console.log("min" + min + "  max:" + max);
     testData.data.forEach(function (res) {
        var old = res.temp
        res.temp =  5 + res.temp*2
        if(res.temp > 10) res.temp = 10
        if(res.temp < 0) res.temp = 0
        if(res.temp < 4.5){
            var a =res.temp - 2.25
            res.temp = 2.25-a
        }
     });
 }

 function difference(num1, num2) {
     return num1 - num2
 }

 function loadJSON(path, success, error) {
     var xhr = new XMLHttpRequest();
     xhr.onreadystatechange = function () {
         if (xhr.readyState === XMLHttpRequest.DONE) {
             if (xhr.status === 200) {
                 if (success)
                     success(JSON.parse(xhr.responseText));
             } else {
                 if (error)
                     error(xhr);
             }
         }
     };
     xhr.open("GET", path, false);
     xhr.send();
 }