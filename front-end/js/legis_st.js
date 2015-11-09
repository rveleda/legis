var apiKey = 'AIzaSyBWdGBJLWQaCoyea6synNCIxrCjWSwhyHk';

var map;

var startMarker, endMarker;
var startAddress, endAddress;
var origin_lat, origin_lng, destination_lat, destination_lng;

var paths = [];
var curPathIndex;
var curStep = 0;
var curPathStepIndex = 0;
var walkerIntervalId;
var pollServerIntervalId;
var drawnLines = [];
var origLat, origLng;
//var timerCount = 0;

var DEFAULT_SCORE = 50;
var LOW_SCORE_THRESHOLD = 0;

var servers = [
  { name: "Toronto", img: "img/tr.jpg", url: "http://142.150.208.151:8080/ca.yorku.asrl.legis.server/rest/directions", localurl: "10.2.7.18", point1: { lat: 43.765545, lng: -79.162050 }, point2: { lat: 42.900100, lng: -79.613552 } },
  { name: "York", img: "img/yk.png", url: "10.7.7.15", localurl: "10.7.7.15", point1: { lat: 46.348499, lng: -79.162050 }, point2: { lat: 43.765545, lng: -79.613552 } },
  { name: "Carleton", img: "img/ct.jpeg", url: "134.117.57.138", localurl: "10.8.7.10", point1: { lat: 46.348499, lng: -72.631295 }, point2: { lat: 42.900100, lng: -79.162050 } },
  { name: "Waterloo", img: "img/wt.jpg", url: "XXX.XXX.XXX.XXX", localurl: "XX.XX.XX.XX", point1: { lat: 46.348499, lng: -79.613552 }, point2: { lat: 42.900100, lng: -81.425033 } },
];

function initialize() {
  var mapOptions = {
    zoom: 13,
    center: {lat: 43.6533103, lng:-79.3827675}
  };

  map = new google.maps.Map(document.getElementById('map'), mapOptions);

  $("#destination").prop('disabled', true);
  getLocation();
}

function showChangedRouteDialog() {
  var infowindow = new google.maps.InfoWindow({
    content: "Changed route!"
  });

  infowindow.open(map, startMarker);

  setTimeout(function () { infowindow.close(); }, 2000);
}

function scoreToColor(score, type) {
  score = (score > 100)? 100 : score;

  if (type == "color") {
    if (score <= 25) {
      var red = 255 - Math.round(score);
      return "rgb(" + red + ",0,0)";
    } else if (score <= 50) {
      var redgreen = Math.round(score) + 155;
      return "rgb(" + redgreen + "," + redgreen + ",0)";
    } else if (score <= 75) {
      var red = Math.round(score) + 155;
      return "rgb(" + red + ",50,0)";
    } else if (score < 25) {
      var green = Math.round(score) + 155;
      return "rgb(0," + green + ",0)";
    }

    var green = Math.round((score*255) / 100);
    var red = Math.round(((100-score) * 255) / 100);
    return "rgb(" + red + "," + green + ",0)";
  } else {
    // gray scale
    var tone = Math.round(score) + 50;
    return "rgb(" + tone + "," + tone + "," + tone + ")";
  }
}

function getServer(lat, lng) {
  for (var i=0; i<servers.length; i++) {
    if (lat >= Math.min(servers[i].point1.lat, servers[i].point2.lat) &&
      lat <= Math.max(servers[i].point1.lat, servers[i].point2.lat) &&
      lng >= Math.min(servers[i].point1.lng, servers[i].point2.lng) &&
      lng <= Math.max(servers[i].point1.lng, servers[i].point2.lng)) {
        return servers[i];
      }
  }
}

function placeStart(lat, lng, address) {
  origin_lat = lat;
  origin_lng = lng;

  var currentLocation = $("#start-location");
  if (address == "") {
    address = startAddress
  }

  startAddress = address;
  currentLocation.html(address + "</br>");
  currentLocation.append("(" + lat.toFixed(6) + ", " + lng.toFixed(6) + ")");

  changeEdge(lat, lng);

  if (startMarker == undefined) {
    startMarker = new google.maps.Marker({
      position: { lat: lat, lng: lng },
      map: map,
      title: "Start"
    });
  } else {
    startMarker.setPosition({ lat: lat, lng: lng });
  }
  return startMarker;
}

function changeEdge(lat, lng) {
  var curServer = getServer(lat, lng);
  $("#logo-edge").attr("src", curServer.img);
}

function placeEnd(lat, lng, address) {
  destination_lat = lat;
  destination_lng = lng;

  var currentDestination = $("#end-location");
  if (address == "") {
    address = endAddress
  }

  endAddress = address;
  currentDestination.html(address + "</br>");
  currentDestination.append("(" + lat.toFixed(6) + ", " + lng.toFixed(6) + ")");

  if (endMarker == undefined) {
    endMarker = new google.maps.Marker({
      position: { lat: lat, lng: lng },
      map: map,
      title: "End"
    });

    var infowindow = new google.maps.InfoWindow({
      content: "End"
    });

    endMarker.addListener('click', function() {
      infowindow.open(map, endMarker);
    });
  } else {
    endMarker.setPosition({ lat: lat, lng: lng });
  }
  return endMarker;
}

function zoomMap() {
  if (startMarker != undefined && endMarker != undefined) {
    var bounds = new google.maps.LatLngBounds();
    bounds.extend(startMarker.getPosition());
    bounds.extend(endMarker.getPosition());
    map.fitBounds(bounds);
  }
}

function animatePane() {
    var move = "+=250px";
    if ($('#pane').css("marginLeft") == "0px") {
      move = "-=250px";
    }
    $('#pane').animate({
      'marginLeft' : move
    }, 1000, function() { google.maps.event.trigger(map, 'resize'); });
}

function getLocation() {
    if (navigator.geolocation) {
        navigator.geolocation.getCurrentPosition(showPosition);
    } else {
        console.log("Geolocation is not supported by this browser");
    }
}

function showPosition(position) {

    map.setCenter({lat:position.coords.latitude, lng:position.coords.longitude});
    origLat = position.coords.latitude;
    origLng = position.coords.longitude;
    //console.log("Latitude: " + position.coords.latitude + " | Longitude: " + position.coords.longitude);

    $.get('https://maps.googleapis.com/maps/api/geocode/json',
      { key: apiKey, latlng:  position.coords.latitude + "," + position.coords.longitude },
      function(data) {

        var address = "";
        if (data != undefined && data.results != undefined && data.results.length > 0) {
          address = data.results[0].formatted_address + "</br>";
        }

        placeStart(position.coords.latitude, position.coords.longitude, address);

        $("#destination").prop('disabled', false);
      }
    );
}

function changeLineColor(line, newColor) {
  line.setOptions({ strokeColor: newColor });
}

function deleteLine(line) {
  line.setMap(null);
}

function clearLines() {
  for (var i=0; i<drawnLines.length; i++) {
    deleteLine(drawnLines[i]);
  }

  drawnLines = [];
  console.log("delete lines");
}

function toRad(Value) {
    /** Converts numeric degrees to radians */
    return Value * Math.PI / 180;
}

function distance(lat1, lng1, lat2, lng2) {
  var earthRadius = 6371000; //meters
  var dLat = toRad(lat2-lat1);
  var dLng = toRad(lng2-lng1);
  var a = Math.sin(dLat/2) * Math.sin(dLat/2) +
             Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) *
             Math.sin(dLng/2) * Math.sin(dLng/2);
  var c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
  var dist = earthRadius * c;

  return dist;
}

function drawRoute(steps, color, z) {
  var curPath = [];

  for (var stepIndex=0; stepIndex<steps.length; stepIndex++) {
    var currentStep = steps[stepIndex];
    var encodedPath = currentStep.polyline.points;
    var decodedPath = google.maps.geometry.encoding.decodePath(encodedPath);
    var score = (currentStep.score != undefined)? currentStep.score : DEFAULT_SCORE;

    var curColor = scoreToColor(score, color);

    var poly = new google.maps.Polyline({ strokeColor: curColor , strokeWeight: 5, map: map, zIndex: z});
    poly.setPath(decodedPath);
    drawnLines.push(poly);

    for (var i=0; i<decodedPath.length; i++) {
      decodedPath[i].stepIndex = stepIndex;
      curPath.push(decodedPath[i]);
    }
  }

  paths.push(curPath);
}

function getConsideredRoutes(data) {
  var curLat = startMarker.getPosition().lat();
  var curLng = startMarker.getPosition().lng();
  var result = [];

  for (var routeIndex = 0; routeIndex<data.routes.length; routeIndex++) {
    isRouteConsidered = false;

    if (data.routes[routeIndex].legs != undefined && data.routes[routeIndex].legs[0].steps != undefined) {
      var minDiff = 100;
      var steps = data.routes[routeIndex].legs[0].steps;

      for (var stepIndex=0; stepIndex<steps.length; stepIndex++) {

        var step = steps[stepIndex];
        var score = (step.score != undefined)? step.score : DEFAULT_SCORE;
        var encodedPath = step.polyline.points;
        var decodedPath = google.maps.geometry.encoding.decodePath(encodedPath);

        // Score too low, don't consider
        if (score < LOW_SCORE_THRESHOLD) {
          console.log("Score too low on route " + routeIndex);
          isRouteConsidered = false;
          minDiff = 100;
          break;
        }

        for (var i=0; i<decodedPath.length; i++) {
          var diffLat = Math.abs(decodedPath[i].lat() - curLat);
          var diffLng = Math.abs(decodedPath[i].lng() - curLng);

          if (diffLat + diffLng < minDiff) {
            minDiff = diffLat + diffLng;
          }
        }
      }

      if (minDiff < 2e-10) {
        isRouteConsidered = true;
      }
    }

    result.push(isRouteConsidered);
  }

  return result;
}

function processRoutesFromServers() {
  var curLat = startMarker.getPosition().lat();
  var curLng = startMarker.getPosition().lng();
  var curServer = getServer(curLat, curLng);

  var url = "http://142.150.208.151:8080/ca.yorku.asrl.legis.server/rest/directions";
  //if (timerCount > 4) {
  //  url = "res/directions_v1_3.json";
  //}

  // Read JSON with response
  $.ajax(
    {
      type: 'GET',
      url: url,
      data: { "origin_long": origLng, "origin_lat": origLat, "destination_long": destination_lng, "destination_lat": destination_lat, "dynamic": false },
      async: false,
      jsonpCallback: 'legis_directions',
      contentType: "application/json",
      dataType: 'jsonp',
      success: function(data) {
        clearLines();

        paths = [];

        if (data != undefined && data.routes != undefined && data.routes.length > 0) {
          var bestRouteIndex = 0;
          var bestRouteAvg = 0;

          var consideredRoutes = getConsideredRoutes(data);
          console.log(consideredRoutes);

          for (var routeIndex = 0; routeIndex<data.routes.length; routeIndex++) {
            var avg = 0;

            if (consideredRoutes[routeIndex] == false) {
              continue;
            }

            if (data.routes[routeIndex].legs != undefined && data.routes[routeIndex].legs[0].steps != undefined) {

              var steps = data.routes[routeIndex].legs[0].steps;
              for (var stepIndex=0; stepIndex<steps.length; stepIndex++) {
                var step = steps[stepIndex];
                var score = (step.score != undefined)? step.score : DEFAULT_SCORE;
                var distance = step.distance.value;
                avg += score / distance;
              }
              avg = avg / steps.length;
              console.log("Average route " + routeIndex + " = " + avg);

              if (avg >= bestRouteAvg) {
                bestRouteIndex = routeIndex;
                bestRouteAvg = avg;
              }
            }
          }

          if (curPathIndex != undefined && curPathIndex != bestRouteIndex) {
              // switch route
              console.log("switch route");
              var curLat = startMarker.getPosition().lat();
              var curLng = startMarker.getPosition().lng();

              var minDiff = 100;
              var minDiffIndex = 0;

              var steps = data.routes[bestRouteIndex].legs[0].steps;
              var count = 0;
              for (var stepIndex=0; stepIndex<steps.length; stepIndex++) {
                var currentStep = steps[stepIndex];
                var encodedPath = currentStep.polyline.points;
                var decodedPath = google.maps.geometry.encoding.decodePath(encodedPath);

                for (var i=0; i<decodedPath.length; i++) {
                  count++;
                  var diffLat = Math.abs(decodedPath[i].lat() - curLat);
                  var diffLng = Math.abs(decodedPath[i].lng() - curLng);

                  if (diffLat + diffLng < minDiff) {
                    minDiff = diffLat + diffLng;
                    minDiffIndex = count;
                  }
                }

                curPathStepIndex = minDiffIndex;
              }

              showChangedRouteDialog();

          }

          curPathIndex = bestRouteIndex;
          //curPathStepIndex = 1;  // Comment if getting the route from origin

          // draw first the alternatives, so the chosen one can be seen
          for (var routeIndex = 0; routeIndex<data.routes.length; routeIndex++) {
            if (routeIndex == bestRouteIndex) {
              continue;
            }

            if (data.routes[routeIndex].legs != undefined && data.routes[routeIndex].legs[0].steps != undefined) {
              drawRoute(data.routes[routeIndex].legs[0].steps, "gray", 1);
            }
          }


          // draw the chosen route
          console.log("Chosen " + bestRouteIndex);
          drawRoute(data.routes[bestRouteIndex].legs[0].steps, "color", 100);
        }
      },
      error: function(e) {
         console.log(e.message);
      }
    });
  //$.getJSON( "http://142.150.208.151:8080/ca.yorku.asrl.legis.server/rest/directions",
  // data: { "origin_long": origin_lng, "origin_lat": origin_lat, "destination_long": destination_lng, "destination_lat": destination_lat },
}

function stopWalking() {
  clearInterval(walkerIntervalId);
}

function walk() {
  // Walker
  walkerIntervalId = setInterval(function() {
    if (paths != undefined && paths.length > 0 && paths[paths.length-1] != undefined) {
      // best route is always the last one added
      if (paths[paths.length-1][curPathStepIndex+1] != undefined) {

        /*var dist = distance(paths[paths.length-1][curPathStepIndex].lat(),
                            paths[paths.length-1][curPathStepIndex].lng(),
                            paths[paths.length-1][curPathStepIndex+1].lat(),
                            paths[paths.length-1][curPathStepIndex+1].lng());
        console.log(dist + " m.");*/

        curPathStepIndex++;
        curStep = paths[paths.length-1][curPathStepIndex].stepIndex;

        placeStart(paths[paths.length-1][curPathStepIndex].lat(), paths[paths.length-1][curPathStepIndex].lng(), "");
      } else {
        stopWalking();
        clearInterval(pollServerIntervalId);
        curPathIndex = undefined;
      }
    }
  }, 200);
}

function pollServerInterval() {
  // initial poll
  pollServer();

  pollServerIntervalId = setInterval(function() {
    //timerCount++;
    pollServer();

  }, 10000);
}

function pollServer() {
  stopWalking();
  processRoutesFromServers();
  walk();
}

$( document ).ready(function() {
  // When a new destination is entered
  $('#destination').on('keypress', function (event) {
       if(event.which === 13){
          //Disable textbox to prevent multiple submit
          $(this).attr("disabled", true);

          // Hide pane
          //animatePane();

          // Get coordinates of the place
          $.get('https://maps.googleapis.com/maps/api/geocode/json',
            { key: apiKey, address: $(this).val(), components: "country:CA" },
            function(data) {
              var address = "";

              if (data != undefined && data.results != undefined && data.results.length > 0 &&
                data.results[0].geometry != undefined && data.results[0].geometry.location != undefined) {

                address = data.results[0].formatted_address + "</br>";

                placeEnd(data.results[0].geometry.location.lat, data.results[0].geometry.location.lng, address);

                // update bounds of map
                zoomMap();

                pollServerInterval();

              }

              $("#destination").prop('disabled', false);

            }
          );
       }
  });

  $('#hide-btn').click(animatePane);
});

$(window).load(initialize);
