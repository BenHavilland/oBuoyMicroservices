// get what we need
var ddbGeo = require('dynamodb-geo');
var AWS = require('aws-sdk');

// handle the http request
exports.handler = function (event, context, callback) {

    // all options are inside handler to prevent
    // any open requests in node allowing graceful exits

    // lock to latest dynamodb API version
    AWS.config.apiVersions = {
      dynamodb: '2012-08-10'
    }; 

    // Let the records flow!
    require('http').globalAgent.maxSockets = require('https').globalAgent.maxSockets = Infinity
    // Instantiate db object
    const ddb = new AWS.DynamoDB();
    // Instantiate db update object
    const ddbClient = new AWS.DynamoDB.DocumentClient();

    // Configuration for a new instance of a GeoDataManager.
    const config = new ddbGeo.GeoDataManagerConfiguration(ddb, 'liveBuoyData');
    // match the hashKeyLength of the Java library this was ported from
    config.hashKeyLength = 3;
    // Instantiate the table manager
    // Each instance represents a table with magic geo queries available
    const buoysManager = new ddbGeo.GeoDataManager(config);

    var operation = event['body-json'].operation;
    var payload = event['body-json'].radiusQuery;
    var radiusQuery = event['body-json'].radiusQuery;

    if ('tableName' in event['body-json']) {
        payload.TableName = event['body-json'].tableName;
    }
 
    switch (operation) {
        case 'create':
            //not allowed on this service
            break;
        case 'read':
            // Make sure the DB is there, if not then this script will timeout
            ddb.waitFor('tableExists', { TableName: config.tableName }).promise()
            .catch(console.warn)
            .then(function () {
                return buoysManager.queryRadius(radiusQuery)
             }).catch(console.warn)
            .then(function(payload){
                // radius queries can't exclude by another key so we
                // filter for the most recent items in this call.
                // this is needed to avoid duplicates when queries are made
                // while the database is updating.

                // check for empty payloads
                if (typeof payload === undefined){
                    payload = {};
                }
                // define results array
                var filteredResults = [];
                for (var p in payload){
                    // add the first item always
                    if (filteredResults.length === 0) {
                        filteredResults.push(payload[p]);
                        continue;
                    }
                    // use prototype.find to reduce any duplicates
                    // that could be caused during a database update
                    var inResults = filteredResults.find(function(value,index) {
                        if (value.STN.S == payload[p].STN.S){
                            // capture only the most recent record

                            // if we have a more recent record replace it in our results
                            if (value.dateepoch.N < payload[p].dateepoch.N){
                                // more recent record found
                                // remove previous record
                                filteredResults.splice( index, 1 ); 
                                // return false so the new record is added
                                return false;
                            }
                            // already in result set so returning true
                            return true;
                        }
                    });
                    // prototype.find returns 'undefined' if there are no hits
                    // if we have no hits then we need to add this to the results
                    if (inResults === undefined) {
                        filteredResults.push(payload[p]);
                    }
                }
                var returnData = {
                    "statusCode": 200,
                    "headers": {
                    "Access-Control-Allow-Origin": "*",
                    "Content-Type": "application/json"
                    },
                    "body": JSON.stringify(filteredResults)
                    }
                callback(null, returnData);
                //context.succeed(returnData);
            }).catch(console.warn);
            break;
        case 'update':
            //not allowed on this service
            break;
        case 'delete':
            //not allowed on this service
            break;
        case 'list':
            //not allowed on this service
            break;
        default:
            // appease the aws proxy 
            var returnData = {
                "statusCode": 200,
                "headers": {
                "Access-Control-Allow-Origin": "*",
                "Content-Type": "application/json"
                },
                "body": ""
                }
            callback(null, returnData);
    }
};
