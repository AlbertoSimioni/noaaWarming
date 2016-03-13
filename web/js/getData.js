 var mapPos = [];
 var heatmapIntensity = 10;
 var min = 1000;
 var max = -1000;

 var testData = {
     max: heatmapIntensity,
     data: []
 }

 function getData(year1, year2) {
     min = 1000;
     max = -1000;
     console.log("getting data");
     loadJSON(year2 + '.json',
         function (data) {
             fillData(data);
             loadJSON(year1 + ".json",
                 function (data2) {
                     calculateDifference(data2);
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

 function fillData(data) {
     data.results.forEach(function (res) {

         mapPos[{
             lat: res.latitude,
             lng: res.longitude
         }] = res.avgTemp;
         //testData.data.push({lat: res.latitude, lng:res.longitude, count: res.avgTemp})
     });
 }

 function calculateDifference(data2) {
     data2.results.forEach(function (res) {
         if (mapPos[{
                 lat: res.latitude,
                 lng: res.longitude
             }] !== "undefined") {
             var diff = difference(mapPos[{
                 lat: res.latitude,
                 lng: res.longitude
             }], res.avgTemp);
             if (diff > max) {

                 max = diff;
                 console.log("max" + max);
                 console.log(mapPos[{
                     lat: res.latitude,
                     lng: res.longitude
                 }] + "  " + res.avgTemp);
             }
             if (diff < min) {
                 min = diff;
             }

             testData.data.push({
                 lat: res.latitude,
                 lng: res.longitude,
                 temp: diff
             });
         }
     });

     min = Math.abs(min);
     max = Math.abs(max);
     max = max + min;
     //console.log("min" + min + "  max:" + max);
     testData.data.forEach(function (res) {
         res.temp = ((res.temp + min) * heatmapIntensity) / max;
         console.log("temp" + res.temp);
     });

     testData.data.forEach(function (res) {
         //console.log(res.temp);
     });

 }

 function difference(num1, num2) {
     return (num1 > num2) ? num1 - num2 : num2 - num1
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
     xhr.open("GET", path, true);
     xhr.send();
 }