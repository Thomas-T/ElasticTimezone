var shapefile = require('shapefile');
var fs = require('fs');
var argv = require('yargs').argv;

var host = argv.host || 'localhost:9200';
var index = argv.index || 'geo-zones-test';
var type = argv.type || 'TimeZone';
var limit = argv.limit || 5;

var elasticsearch = require('elasticsearch');
var client = new elasticsearch.Client({
  host: host,
  log: 'info',
  requestTimeout: 60000
});



function createLine(bulk, feature) {
  var line = '';
  var meta = {index: { _index: index, _type: 'TimeZone' }};
  var body = {};
  body.name = feature.properties.TZID;
  body.location = {
    type: feature.geometry.type.toLowerCase(),
    coordinates: feature.geometry.coordinates
  };
  bulk.push(meta);
  bulk.push(body);
}


var startAt = new Date();

function endOfProgram() {
  console.log('end of program');
  client.close();
}

function timeStep(nbFeatures, pass) {

  var elapsed = new Date() - startAt;
  var prog = (limit*pass) / nbFeatures;
  var estimated = 100 / (prog*100) * elapsed;

  console.log('pass', pass, 'prog', Math.round(prog*100),'%', 'remaining',  Math.round((estimated-elapsed)/1000),'s');
}

function bulkify(features, pass) {
  console.log('bulkify', pass);
  var start = limit * (pass-1);
  var end = limit * pass;

  if(start > features.length) {
    endOfProgram();
    return;
  }

  if(end  > features.length) {
    end = features.length;
  }

  var bulk = [];

  while(start < end) {
    createLine(bulk, features[start]);
    start++;
  }

  client.bulk({
    body: bulk
    }, function (err, resp) {
      if(err) {
        console.log('error in bulk', err);
        endOfProgram();
        return;
      }

      timeStep(features.length, pass);

      pass++;

      bulkify(features, pass);

    });

}


shapefile.read('tz_world_mp.shp', function(error, collection){
  if(error) {
    console.log(error);
    return;
  }

  console.log('delete mapping if exist');
  client.indices.deleteMapping({index:index,type:'TimeZone'}, function(err){
    if(err) {
      console.log('mapping didnt existed');
    }
    console.log('create mapping');
    client.indices.putMapping({
        index: index,
        type:'TimeZone',
        body: {
          properties: {
            name: { type: "string" },
            location: {
              type: "geo_shape",
              tree: "quadtree",
              precision: "1m",
              distance_error_pct:0.005
            }
          }
        }

      }, function(err){
      if(err) {
        console.log('error creating mapping',err);
        endOfProgram();
        return;
      }
      console.log('mapping created');
      bulkify(collection.features, 1);

    });

  });


});
