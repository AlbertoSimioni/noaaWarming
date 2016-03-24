 var heatmapIntensity = 10;

 function getData(year1, year2, metric) {
    mapPos = {}
     testData = {
        max: heatmapIntensity,
        data: []
     }
     var folder = null;
     if(metric === "avgTemp" || metric === "stddev") folder = "avgs"
     if(metric === "minTemp" || metric === "maxTemp") folder = "minmax"
     loadJSON("results/"+folder+"/"+year2 + '.json', //second year data loading
         function (data) {
             fillData(data,mapPos,metric);
             loadJSON("results/"+folder+"/"+year1 + ".json",  //first year data loading
                 function (data2) {
                     calculateDifference(data2,mapPos,testData,metric); //compute differences
                     insertData(testData); //inserts the data in the heatmap
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
     });
 }

 function calculateDifference(data2,mapPos,testData,metric) {
     data2.results.forEach(function (res) {
         if (mapPos[res.lat +"_" + res.long] !== undefined) {
             var diff = difference(mapPos[res.lat +"_" + res.long], res[metric]);
             //repeated for each region
             testData.data.push({
                 lat: res.lat,
                 lng: res.long,
                 temp: diff
             });
         }
     });
     //calculating the value to insert in the heatmap for each single data
     //the values are between [0,10]
     //if the difference is >= than 2.5 then it will have value 10
     //The code is a bit weird in order to create the values for the gradient used in the heatmap
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