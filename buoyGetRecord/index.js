// get what we need
const ddbGeo = require('dynamodb-geo');
const AWS = require('aws-sdk');

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
// Each instance represents a table with magic geo queries available
const config = new ddbGeo.GeoDataManagerConfiguration(ddb, 'liveBuoyData');
// match the hashKeyLength of the Java library this was ported from
config.hashKeyLength = 6;
// Instantiate the table manager
const buoysManager = new ddbGeo.GeoDataManager(config);

// handle the http request
exports.handler = function (event, context, callback) {

    // set the buoy station from event payload
    var station = event['body-json'].STN || undefined;

    // friendly response on missing STN value
    if (typeof station === undefined){
        var returnData = {
        "statusCode": 200,
        "headers": {
        "Access-Control-Allow-Origin": "*",
        "Content-Type": "application/json"
        },
        "body": "enter value for STN { 'STN': 'XXYYZ' }"
        }
        callback(null,returnData);
    }

    // get operation from payload
    var operation = event['body-json'].operation;

    switch (operation) {
        case 'create':
            //not allowed on this service
            break;
        case 'read':
            // structure the query the dynamodb way
            var params = {
                TableName:config.tableName,
                ScanIndexForward: false,
                IndexName:"active-dateepoch-index",
                KeyConditionExpression:"active = :a",
                FilterExpression: "STN = :s",
                ExpressionAttributeValues:{
                    ":a": 1,
                    ":s": String(station)
                }
            };
            // Make sure the DB and table is there, if not then this script will timeout
            ddb.waitFor('tableExists', { TableName: config.tableName }).promise()
            .catch(console.warn)
            .then(function (){
                // table exists, db is ready, time to fire off the query
                ddbClient.query(params).promise()
                .catch(console.warn)
                .then(function(data) {
                    var returnData = {
                        "statusCode": 200,
                        "headers": {
                            "Access-Control-Allow-Origin": "*",
                            "Content-Type": "application/json"
                        },
                        "body": JSON.stringify([data.Items[0]])// only return 1
                        }
                    callback(null,returnData);
                });
            })
            .catch(console.warn);
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
}
