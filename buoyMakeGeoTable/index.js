// get what we need
const ddbGeo = require('dynamodb-geo');
const AWS = require('aws-sdk');
const uuid = require('uuid');
const request=require('request');
const csv=require('csvtojson');
const http = require('http');

// lock to latest dynamodb API version
AWS.config.apiVersions = {
  dynamodb: '2012-08-10'
}; 

// Let the records flow!
require('http').globalAgent.maxSockets = require('https').globalAgent.maxSockets = Infinity
// Instantiate db object
const ddb = new AWS.DynamoDB();
// Configuration for a new instance of a GeoDataManager.
// Each instance represents a table with magic geo queries available
const config = new ddbGeo.GeoDataManagerConfiguration(ddb, 'liveBuoyData');
// match the hashKeyLength of the Java library this was ported from
config.hashKeyLength = 3;
// Instantiate the table manager
const buoysManager = new ddbGeo.GeoDataManager(config);
// Use GeoTableUtil to help construct a CreateTableInput.
// This is used if no table exists
const createTableInput = ddbGeo.GeoTableUtil.getCreateTableRequest(config);
// Configure options for new table
createTableInput.ProvisionedThroughput.ReadCapacityUnits = 75;
createTableInput.ProvisionedThroughput.WriteCapacityUnits = 50;

exports.handler = function (event, context, callback) {
    // deployment helper, if the table's not there let's create it
    ddb.createTable(createTableInput).promise()
        .catch(function(error){
            console.warn(error);
            // table already exists, so we're updating it
        })// Wait for it to become ready
        .then(function () { return ddb.waitFor('tableExists', { TableName: config.tableName }).promise() })
        .then(function (){
            // table is ready so we need to get our data to insert

            // vendor data path
            // rss feed listed in requirements was inadequate for providing
            // all data.  latest obs feed txt was used.
            var options = {
              host: 'www.ndbc.noaa.gov',
              path: '/data/latest_obs/latest_obs.txt'
            };

            const operation = event.operation;
            const payload = event.payload;

            if (event.tableName) {
                payload.TableName = event.tableName;
            }

            var processData = function(response) {
                // we have data, now we will process
                var str = '';
                
                //another chunk of data has been recieved, so append it to `str`
                response.on('data', function (chunk) {
                    str += chunk;
                });

                //the whole response has been recieved
                response.on('end', function () {

                    // This text file has a 2 line header that we must normalize
                    // Normalizing: break the textblock into an array of lines
                    var lines = str.split('\n');
                    // remove top 2 lines
                    lines.splice(0,2);
                    // add in our headers
                    var strHeaders = 'STN,LAT,LON,YYYY,MM,DD,hh,mm,WDIR,WSPD,GST,WVHT,DPD,APD,MWD,PRES,PTDY,ATMP,WTMP,DEWP,VIS,TIDE\n'
                    lines.unshift(strHeaders);
                    // join the array back into a single string
                    var strNoHeader = lines.join('\n');

                    // We have a dumb amount of whitespace in this buoy
                    // data file that has unpredictable lengths
                    // this is the *fastest way to fix that
                    // yes it's 2 while loops but this is the way to
                    // go via benchmarking
                    while (strNoHeader.indexOf("  ") !== -1) {
                        strNoHeader = strNoHeader.replace(/  /g, " ");
                    }

                    // make this a CSV not a SpaceSV
                    while (strNoHeader.indexOf(" ") !== -1) {
                        strNoHeader = strNoHeader.replace(/ /g, ",");
                    }

                    // text file is now a normalized CSV!
                    // let's parse this cleanly into JSON with fast csv lib
                    // so we can store this puppy with geolocation magic
                    csv()
                    .fromString(strNoHeader)
                    .on('end_parsed',function (jsonData){
                        const putBuoyPointInputs = jsonData.map(function (buoy) {
                            isoFormattedDate = new Date(String(buoy.YYYY+'-'+buoy.MM+'-'+buoy.DD+'T'+buoy.hh+':'+buoy.mm+':'+'00Z'));
                            return {
                                RangeKeyValue: { S: uuid.v4() }, // Use this to ensure uniqueness of the hash/range pairs.
                                GeoPoint: {
                                    latitude: String(buoy.LAT),
                                    longitude: String(buoy.LON)
                                },
                                PutItemInput: {
                                    Item: {
                                        active: { N: String(1)},
                                        dateepoch: { N: String(isoFormattedDate.getTime()) },
                                        STN: { S: String(buoy.STN) },
                                        DATE: { S: String(buoy.YYYY+'-'+buoy.MM+'-'+buoy.DD+'T'+buoy.hh+':'+buoy.mm+':'+'00Z') },
                                        WSPD: { S: String(buoy.WSPD) },
                                        GST: { S: String(buoy.GST) },
                                        DPD: { S: String(buoy.APD) },
                                        WVHT: { S: String(buoy.WVHT) },
                                        MWD: { S: String(buoy.MWD) },
                                        PRES: { S: String(buoy.PRES) },
                                        PTDY: { S: String(buoy.PTDY) },
                                        ATMP: { S: String(buoy.ATMP) },
                                        WTMP: { S: String(buoy.WTMP) },
                                        DEWP: { S: String(buoy.DEWP) },
                                        VIS: { S: String(buoy.VIS) },
                                        TIDE: { S: String(buoy.TIDE) }
                                    }
                                }
                            }
                        });

                        // set our dynamodb batch write options
                        const BATCH_SIZE = 25;//25 is max for dynamodb
                        const WAIT_BETWEEN_BATCHES_MS = 200;

                        // prep our promises
                        var currentBatch = 1;
                        var putBuoyPointInputsLen = 0;
                        
                        // create the batch write promise
                        // allowing us to write BATCH_SIZE at once
                        // resumeWritingBuoyData is from dynamodb geo lib doc
                        function resumeWritingBuoyData() {
                            if (putBuoyPointInputsLen === 0){
                                putBuoyPointInputsLen = putBuoyPointInputs.length;
                            }
                            
                            if (putBuoyPointInputs.length === 0) {
                                return Promise.resolve();
                            }
                            const thisBatch = [];
                            for (var i = 0, itemToAdd = null; i < BATCH_SIZE && (itemToAdd = putBuoyPointInputs.shift()); i++) {
                                thisBatch.push(itemToAdd);
                            }
                            console.log('writing batch ' + (currentBatch++) + '/' + Math.ceil(putBuoyPointInputsLen / BATCH_SIZE));
                            return buoysManager.batchWritePoints(thisBatch).promise()
                                .then(function () {
                                    return new Promise(function (resolve) {
                                        setInterval(resolve,WAIT_BETWEEN_BATCHES_MS);
                                    });
                                })
                                .then(function () {
                                    return resumeWritingBuoyData();
                                });
                        };
                        // kick off the batch write promise
                        return resumeWritingBuoyData().catch(function (error) {
                            console.warn(error);
                        })
                        .catch(console.warn) //catch any errors and return happy
                        .then(function () {
                            return context.succeed("success");
                        });
                    });
                });
            }
            // this is where the app kicks off the run of the code above
            // grab data from vendor and fire off the callback -> processData
            http.request(options, processData).end();
        });
}
