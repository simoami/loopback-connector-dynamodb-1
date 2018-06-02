"use strict";
var g = require("strong-globalize")();
var Connector = require("loopback-connector").Connector;

// It doesn't include sharding or auto partitioning of items above 400kb

/**
 * Module dependencies
 */
var AWS = require("aws-sdk");
var DocClient = AWS.DynamoDB.DocumentClient;
var helper = require("./helper.js");
var async = require("async");
var EventEmitter = require("events").EventEmitter;
var util = require("util");
var DataAccessObject = require("./dao/dao.js");
var debug = require("debug")("loopback:connector:dynamodb");


/**
 * The constructor for DynamoDB connector
 * @param {Object} settings The settings object
 * @param {DataSource} dataSource The data source instance
 * @constructor
 */
function DynamoDB(s, dataSource) {
    if (!AWS) {
        throw new Error("AWS SDK not installed. Please run npm install aws-sdk");
    }

    this.DataAccessObject = DataAccessObject;
    this.name = "dynamodb";
    this._models = {};
    this._tables = {};
    this._attributeSpecs = [];
    // Connect to dynamodb server
    var dynamodb;
    // Try to read accessKeyId and secretAccessKey from environment variables
    if ((process.env.AWS_ACCESS_KEY_ID !== undefined) && (process.env.AWS_SECRET_ACCESS_KEY !== undefined)) {
        debug("Credentials selected from environment variables");
        AWS.config.update({
            region: s.region,
            maxRetries: s.maxRetries
        });
        dynamodb = new AWS.DynamoDB();
    } else {
        debug("Credentials not found in environment variables");
        try {
            AWS.config.loadFromPath("credentials.json");
            debug("Loading credentials from file");
            dynamodb = new AWS.DynamoDB();
        } catch (e) {
            debug("Cannot find credentials file");
            debug("Using settings from schema");
            AWS.config.update({
                accessKeyId: s.accessKeyId,
                secretAccessKey: s.secretAccessKey,
                region: s.region,
                maxRetries: s.maxRetries
            });
            if (s.useAWS) {
                dynamodb = new AWS.DynamoDB();
            }
            else {
                dynamodb = new AWS.DynamoDB({
                    endpoint: new AWS.Endpoint("http://" + s.host + ":" + s.port)
                });
            }
        }
    }

    this.client = dynamodb; // Used by instance methods
    this.docClient = new DocClient({
        service: dynamodb
    });

    this.emitter = new EventEmitter();
}

util.inherits(DynamoDB, Connector);


/**
 * Initialize the DynamoDB connector for the given data source
 *
 * @param {DataSource} ds The data source instance
 * @param {Function} [cb] The cb function
 */
exports.initialize = function initializeSchema(ds, cb) {
    // s stores the ds settings
    var s = ds.settings;
    if (ds.settings) {
        s.host = ds.settings.host || "localhost";
        s.port = ds.settings.port || 8000;
        s.region = ds.settings.region || "us-east-1";
        s.accessKeyId = ds.settings.accessKeyId || "fake";
        s.secretAccessKey = ds.settings.secretAccessKey || "fake";
        s.maxRetries = ds.settings.maxRetries || 0;
    } else {
        s.region = "us-east-1";
    }

    debug("Initializing dynamodb adapter");
    ds.connector = new DynamoDB(s, ds);
    ds.connector.dataSource = ds;

    if (cb) {
        cb();
    }
};


/*
  Assign Attribute Definitions
  and KeySchema based on the keys
*/
function AssignKeys(name, type, settings) {
    var attr = {};
    var tempString;
    var aType;

    attr.keyType = name.keyType;
    tempString = (name.type).toString();
    aType = tempString.match(/\w+(?=\(\))/)[0];
    aType = aType.toLowerCase();
    attr.attributeType = helper.TypeLookup(aType);
    return attr;
}

/**
 * count number of properties in object
 */




function countProperties(obj) {
    return Object.keys(obj).length;
}

/*
DynamoDB.prototype.isWhereByGivenId = function isWhereByGivenId(Model, where, idValues) {
    var keys = Object.keys(where);
    if (keys.length != 1) return false;
    var pk = idName(Model);
    if (keys[0] !== pk) return false;

    return where[pk] === idValue;
};*/

/**
  Record current time in milliseconds
*/
function startTimer() {
    let timeNow = new Date().getTime();
    return timeNow;
}
/**
  Given start time, return a string containing time difference in ms
*/
function stopTimer(timeStart) {
    return "[" + String(new Date().getTime() - timeStart) + " ms]";
}
/**
 * Create a table based on hashkey, sortkey specifications
 * @param  {object} dynamodb        : adapter
 * @param  {object} tableParams     : KeySchema & other attrs
 * @param {Boolean} tableStatusWait : If true, wait for table to become active
 * @param {Number} timeInterval     : Check table status after `timeInterval` milliseconds
 * @param {function} callback       : Callback function
 */
function createTable(dynamodb, tableParams, tableStatusWait, timeInterval, callback) {
    var tableExists = false;
    var tableStatusFlag = false;
    dynamodb.listTables(function(err, data) {
        if (err || !data) {
            debug("-------Error while fetching tables from server. Please check your connection settings & AWS config--------");
            callback(err, null);
            return;
        } else {
            // Boolean variable to check if table already exists.
            var existingTableNames = data.TableNames;
            existingTableNames.forEach(function(existingTableName) {
                if (tableParams.TableName === existingTableName) {
                    tableExists = true;
                    debug("TABLE %s FOUND IN DATABASE", existingTableName);
                }
            });
            // If table exists do not create new table
            if (tableExists === false) {
                // DynamoDB will throw error saying table does not exist
                debug("CREATING TABLE: %s IN DYNAMODB", tableParams.TableName);
                dynamodb.createTable(tableParams, function(err, data) {
                    if (err || !data) {
                        callback(err, null);
                        return;
                    } else {
                        debug("TABLE CREATED");
                        if (tableStatusWait) {

                            async.whilst(function() {
                                return !tableStatusFlag;

                            }, function(innerCallback) {
                                debug("Checking Table Status");
                                dynamodb.describeTable({
                                    TableName: tableParams.TableName
                                }, function(err, tableData) {
                                    if (err) {
                                        innerCallback(err);
                                    } else if (tableData.Table.TableStatus === "ACTIVE") {
                                        debug("Table Status is `ACTIVE`");
                                        tableStatusFlag = true;
                                        innerCallback(null);
                                    } else {
                                        setTimeout(innerCallback, timeInterval);
                                    }
                                });

                            }, function(err) {
                                if (err) {
                                    callback(err, null);
                                } else {
                                    callback(null, "active");
                                }
                            }.bind(this));
                        }
                    } // successful response
                }.bind(this));
            } else {
                callback(null, "done");
            }
        }
    });
}


// Check if object is empty
function isEmpty(obj) {
    var hasOwnProperty = Object.prototype.hasOwnProperty;
    // null and undefined are "empty"
    if (obj === null) {
        return true;
    }

    // Assume if it has a length property with a non-zero value
    // that that property is correct.
    if (obj.length > 0) {
        return false;
    }
    if (obj.length === 0) {
        return true;
    }

    // Otherwise, does it have any properties of its own?
    // Note that this doesn't handle
    // toString and valueOf enumeration bugs in IE < 9
    for (var key in obj) {
        if (hasOwnProperty.call(obj, key)) return false;
    }

    return true;
}


/**
 * Define schema and create table with hash and sort keys
 * @param  {object} descr : description specified in the schema
 */
DynamoDB.prototype.define = function(descr) {
    var timeStart = startTimer();
    if (!descr.settings) descr.settings = {};
    var modelName = descr.model.modelName;
    var emitter = this.emitter;
    // Set Read & Write Capacity Units
    this._models[modelName] = descr;
    descr.ReadCapacityUnits = descr.settings.ReadCapacityUnits || 5;
    descr.WriteCapacityUnits = descr.settings.WriteCapacityUnits || 10;

    descr.localIndexes = {};
    descr.globalIndexes = {};

    var timeInterval, tableStatusWait;
    // Wait for table to become active?
    if (descr.settings.tableStatus) {
        tableStatusWait = descr.settings.tableStatus.waitTillActive;
        if (tableStatusWait === undefined) {
            tableStatusWait = true;
        }
        timeInterval = descr.settings.tableStatus.timeInterval || 5000;
    } else {
        tableStatusWait = true;
        timeInterval = 5000;
    }

    // Create table now with the hash and sort index.
    var properties = descr.properties;
    // Iterate through properties and find index
    var tableParams = {};
    tableParams.AttributeDefinitions = [];
    tableParams.KeySchema = [];
    let LocalSecondaryIndexes = [];
    let GlobalSecondaryIndexes = [];
    this._attributeSpecs[modelName] = {};
    // Temporary object to store read and write capacity units for breakable attrs
    var rcus = {};
    var wcus = {};

    /*
      Build KeySchema for the table based on schema definitions.
     */
    for (var key in properties) {
        // Assign breakers, limits or whatever other properties
        // are specified first
        // Store the type of attributes in _attributeSpecs. This is
        // quite helpful to do Date & Boolean conversions later
        // on.
        var tempString = (properties[key].type).toString();
        var aType = tempString.match(/\w+(?=\(\))/)[0];
        aType = aType.toLowerCase();
        this._attributeSpecs[modelName][key] = aType;

        // Check if UUID is set to be true for HASH KEY attribute
        if (properties[key].keyType === "hash") {
            if (properties[key].uuid === true) {
                if (key !== "id") {
                    throw new Error("UUID generation is only allowed for attribute name id");
                } else {
                    this._models[modelName].hashKeyUUID = true;
                    debug("Hash key UUID generation: TRUE");
                }

            } else {
                this._models[modelName].hashKeyUUID = false;
            }
        }
        // Following code is applicable only for keys
        if (properties[key].keyType !== undefined) {
            var attrs = AssignKeys(properties[key]);
            debug("CHECKING: %s", attrs.keyType);
            // The keys have come! Add to tableParams
            // Add Attribute Definitions
            // HASH primary key?
            if (attrs.keyType === "hash") {
                this._models[modelName].hashKey = key;
                debug("HASH KEY: %s", key);
                tableParams.KeySchema.push({
                    AttributeName: key,
                    KeyType: "HASH"
                });
                tableParams.AttributeDefinitions.push({
                    AttributeName: key,
                    AttributeType: attrs.attributeType
                });
            }
            // Sort primary key?
            if (attrs.keyType === "sort") {
                this._models[modelName].sortKey = key;
                debug("sortkey KEY: %s", key);
                tableParams.KeySchema.push({
                    AttributeName: key,
                    KeyType: "SORT"
                });
                tableParams.AttributeDefinitions.push({
                    AttributeName: key,
                    AttributeType: attrs.attributeType
                });
            }
        }

        if (properties[key].index !== undefined) {
            if (properties[key].index.local !== undefined) {
                attrs = AssignKeys(properties[key]);
                let index = properties[key].index.local;
                let keyName = key + "LocalIndex";
                let localIndex = {
                    IndexName: keyName,
                    KeySchema: [{
                        AttributeName: this._models[modelName].hashKey,
                        KeyType: "HASH"
                    }, {
                        AttributeName: this._models[modelName].sortKey,
                        KeyType: "SORT"
                    }],
                    Projection: {}
                };

                if (index.project) {
                    if (util.isArray(index.project)) {
                        localIndex.Projection = {
                            ProjectionType: "INCLUDE",
                            NonKeyAttributes: index.project
                        };
                    } else {
                        localIndex.Projection = {
                            ProjectionType: "ALL"
                        };
                    }
                } else {
                    localIndex.Projection = {
                        ProjectionType: "KEYS_ONLY"
                    };
                }
                LocalSecondaryIndexes.push(localIndex);
                tableParams.AttributeDefinitions.push({
                    AttributeName: key,
                    AttributeType: attrs.attributeType
                });
                this._models[modelName].localIndexes[key] = {
                    hash: this._models[modelName].hashKey,
                    sort: key,
                    IndexName: keyName
                };
            }
            if (properties[key].index.global !== undefined) {
                attrs = AssignKeys(properties[key]);
                let index = properties[key].index.global;
                let keyName = key + "GlobalIndex";
                var globalIndex = {
                    IndexName: keyName,
                    KeySchema: [{
                            AttributeName: key,
                            KeyType: "HASH"
                        },
                        {
                            AttributeName: index.sortKey,
                            KeyType: "SORT"
                        }
                    ],
                    ProvisionedThroughput: {
                        ReadCapacityUnits: index.throughput.read || 5,
                        WriteCapacityUnits: index.throughput.write || 10
                    }
                };

                if (index.project) {
                    if (util.isArray(index.project)) {
                        globalIndex.Projection = {
                            ProjectionType: "INCLUDE",
                            NonKeyAttributes: index.project
                        };
                    } else {
                        globalIndex.Projection = {
                            ProjectionType: "ALL"
                        };
                    }
                } else {
                    globalIndex.Projection = {
                        ProjectionType: "KEYS_ONLY"
                    };
                }
                GlobalSecondaryIndexes.push(globalIndex);
                tableParams.AttributeDefinitions.push({
                    AttributeName: key,
                    AttributeType: attrs.attributeType
                });
                this._models[modelName].globalIndexes[key] = {
                    hash: key,
                    sort: index.sortKey,
                    IndexName: keyName
                };
            }
        }
    }
    if (LocalSecondaryIndexes.length) {
        tableParams.LocalSecondaryIndexes = LocalSecondaryIndexes;
    }
    if (GlobalSecondaryIndexes.length) {
        tableParams.GlobalSecondaryIndexes = GlobalSecondaryIndexes;
    }

    tableParams.ProvisionedThroughput = {
        ReadCapacityUnits: this._models[modelName].ReadCapacityUnits,
        WriteCapacityUnits: this._models[modelName].WriteCapacityUnits
    };
    debug("Read Capacity Units: %s", tableParams.ProvisionedThroughput.ReadCapacityUnits);
    debug("Write Capacity Units: %s", tableParams.ProvisionedThroughput.WriteCapacityUnits);


    /*
      JugglingDB expects an id attribute in return even if a hash key is not specified. Hence
      if idinjection is false and the hash key is not defined in the schema, create an attribute called id, set it as hashkey.
     */
    if ((this._models[modelName].hashKey === undefined)/* && (properties.id === undefined)*/) {
        this._models[modelName].hashKey = "id";
        this._models[modelName].hashKeyUUID = true;
        this._attributeSpecs[modelName][this._models[modelName].hashKey] = "string";
        tableParams.KeySchema.push({
            AttributeName: "id",
            KeyType: "HASH"
        });
        tableParams.AttributeDefinitions.push({
            AttributeName: "id",
            AttributeType: "S"
        });
    }

    // TODO: If there are breakable attrs with sharding set to true, create the
    // extra tables now
    var _dynamodb = this.client;
    var attributeSpecs = this._attributeSpecs[modelName];
    var ReadCapacityUnits = this._models[modelName].ReadCapacityUnits;
    var WriteCapacityUnits = this._models[modelName].WriteCapacityUnits;
    var hashKey = this._models[modelName].hashKey;
    debug("Model Name: %s", this._models[modelName].hashKey);
    // Assign table name
    tableParams.TableName = descr.settings.table || modelName;
    debug("Table Name:", tableParams.TableName);
    // Add this to _tables so that instance methods can use it.
    this._tables[modelName] = tableParams.TableName;
    // Create main table function
    //debug("data:", data);
    createTable(_dynamodb, tableParams, tableStatusWait, timeInterval, function(err, data) {
        if (err || !data) {
            var tempString = "while creating table: " + tableParams.TableName + " => " + err.message.toString();
            throw new Error(tempString);
        } else {

        }
    });
    debug("Defining model: ", modelName, stopTimer(timeStart).bold.cyan);
};
/**
 * Creates a DynamoDB compatible representation
 * of arrays, objects and primitives.
 * @param {object} data: Object to be converted
 * @return {object} DynamoDB compatible JSON
 */
function DynamoFromJSON(data) {
    var obj;
    /*
      If data is an array, loop through each member
      of the array, and call objToDB on the element
      e.g ["someword",20] --> [ {'S': 'someword'} , {'N' : '20'}]
     */
    if (data instanceof Array) {
        obj = [];
        data.forEach(function(dataElement) {
            // If string is empty, assign it as
            // "null".
            if (dataElement === "") {
                dataElement = "empty";
            }
            if (dataElement instanceof Date) {
                dataElement = Number(dataElement);
            }
            obj.push(helper.objToDB(dataElement));
        });
    }
    /*
      If data is an object, loop through each member
      of the object, and call objToDB on the element
      e.g { age: 20 } --> { age: {'N' : '20'} }
     */
    else if ((data instanceof Object) && (data instanceof Date !== true)) {
        obj = {};
        for (var key in data) {
            if (data.hasOwnProperty(key)) {
                // If string is empty, assign it as
                // "null".
                if (data[key] === undefined) {
                    data[key] = "undefined";
                }
                if (data[key] === null) {
                    data[key] = "null";
                }
                if (data[key] === "") {
                    data[key] = "empty";
                }
                // If Date convert to number
                if (data[key] instanceof Date) {
                    data[key] = Number(data[key]);
                }
                obj[key] = helper.objToDB(data[key]);
            }
        }
        /*
    If data is a number, or string call objToDB on the element
    e.g 20 --> {'N' : '20'}
   */
    } else {

        // If string is empty, assign it as
        // "empty".
        if (data === null) {
            data = "null";
        }
        if (data === undefined) {
            data = "undefined";
        }
        if (data === "") {
            data = "empty";
        }
        // If Date convert to number
        if (data instanceof Date) {
            data = Number(data);
        }
        obj = helper.objToDB(data);
    }
    return obj;
}

function KeyOperatorLookup(operator) {
    let value;
    switch (operator) {
        case "=":
            value = "=";
            break;
        case "lt":
            value = "<";
            break;
        case "lte":
            value = "<=";
            break;
        case "gt":
            value = ">";
            break;
        case "gte":
            value = ">=";
            break;
        case "between":
            value = "BETWEEN";
            break;
        default:
            value = "=";
            break;
    }
    return value;
}

/**
 * Converts jugglingdb operators like 'gt' to DynamoDB form 'GT'
 * @param {string} DynamoDB comparison operator
 */
function OperatorLookup(operator) {
    if (operator === "inq") {
        operator = "in";
    }
    return operator.toUpperCase();
}

DynamoDB.prototype.defineProperty = function(modelName, prop, params) {
    this.getModel(modelName).properties[prop] = params;
};

DynamoDB.prototype.tables = function(name) {
    if (!this._tables[name]) {
        this._tables[name] = name;
    }
    return this._tables[name];
};
/**
 * Create a new item or replace/update it if it exists
 * @param  {object}   model
 * @param  {object}   data   : key,value pairs of new model object
 * @param  {Function} callback
 */
DynamoDB.prototype.create = function(modelName, data, options, callback) {
    var timerStart = startTimer();
    debug("data sent to create: %s", JSON.stringify(data));

    const model = this.getModel(modelName);
    var hashKey = model.hashKey;
    var err;
    var originalData = {};
    if (model.hashKeyUUID === true) {
        hashKey = "id";
        data.id = helper.UUID();
    }
    // Copy all attrs from data to originalData
    for (var key in data) {
        originalData[key] = data[key];
    }

    if (data[hashKey] === undefined) {
        err = new Error("Hash Key `" + hashKey + "` is undefined.");
        callback(err, null);
        return;
    }
    if (data[hashKey] === null) {
        err = new Error("Hash Key `" + hashKey + "` cannot be NULL.");
        callback(err, null);
        return;
    }

    var queryString = "CREATE ITEM IN TABLE " + this.tables(modelName);
    var tableParams = {};
    tableParams.TableName = this.tables(modelName);
    tableParams.ReturnConsumedCapacity = "TOTAL";

    var tempString = "INSERT ITEM INTO TABLE: " + tableParams.TableName;
    debug("%s", tempString);
    tableParams.Item = data;
    this.docClient.put(tableParams, function(err, res) {
        if (err || !res) {
            callback(err, null);
            return;
        } else {
            debug(queryString.blue, stopTimer(timerStart).bold.cyan);
            //originalData.id = originalData[id];
            callback(null, originalData.id);
            return;
        }
    }.bind(this));
};
/**
 * Function that performs query operation on dynamodb
 * @param  {object} modelName
 * @param  {object} filter             : Query filter
 * @param  {Number/String} hashKey     : Hash Key
 * @param  {object} sortKey           : Sort Key
 * @param  {String} queryString        : The query string (used for console logs)
 * @param  {Number} timeStart          : Start time of query operation in milliseconds
 * @return {object}                    : Final query object to be sent to dynamodb
 */
function query(modelName, filter, model, queryString, timeStart) {
    // Table parameters to do the query/scan
    let hashKey = model.hashKey;
    let sortKey = model.sortKey;
    let localKeys = model.localIndexes;
    let globalKeys = model.globalIndexes;

    var tableParams = {};
    // Define the filter if it does not exist
    if (!filter) {
        filter = {};
    }
    // Initialize query as an empty object
    var queryObj = {};
    // Construct query for amazon DynamoDB
    // Set queryfileter to empty object
    tableParams.ExpressionAttributeNames = {};
    let ExpressionAttributeNames = {};
    tableParams.ExpressionAttributeValues = {};
    tableParams.KeyConditionExpression = "";

    let KeyConditionExpression = [];
    let FilterExpression = [];
    //let ExpressionAttributeValues = {};

    // If a where clause exists in the query, extract
    // the conditions from it.
    if (filter.where) {
        queryString = queryString + " WHERE ";
        for (var key in filter.where) {
            var condition = filter.where[key];
            let keyName = "#" + key.slice(0, 1).toUpperCase();
            if (key.toUpperCase() !== "OR") {
                if (tableParams.ExpressionAttributeNames[keyName] == undefined) {
                    tableParams.ExpressionAttributeNames[keyName] = key;
                } else {
                    let i = 1;
                    while (tableParams.ExpressionAttributeNames[keyName] != undefined) {
                        keyName = "#" + key.slice(0, i);
                        i++;
                    }
                    keyName = "#" + key.slice(0, i).toUpperCase();
                    tableParams.ExpressionAttributeNames[keyName] = key;
                }
                
                ExpressionAttributeNames[key] = keyName;
            }
            

            var ValueExpression = ":" + key;
            var insideKey = null;

            if (key === hashKey || (globalKeys[key] && globalKeys[key].hash === key) ||
                (localKeys[key] && localKeys[key].hash === key)) {
                if (condition && condition.constructor.name === "Object") {} else if (condition && condition.constructor.name === "Array") {} else {
                    KeyConditionExpression[0] = keyName + " = " + ValueExpression;
                    tableParams.ExpressionAttributeValues[ValueExpression] = condition;
                    if (globalKeys[key] && globalKeys[key].hash === key) {
                        tableParams.IndexName = globalKeys[key].IndexName;
                    } else if (localKeys[key] && localKeys[key].hash === key) {
                        tableParams.IndexName = localKeys[key].IndexName;
                    }
                }
            } else if (key === sortKey || (globalKeys[key] && globalKeys[key].sort === key) ||
                (localKeys[key] && localKeys[key].sort === key)) {
                if (condition && condition.constructor.name === "Object") {
                    insideKey = Object.keys(condition)[0];
                    condition = condition[insideKey];
                    let operator = KeyOperatorLookup(insideKey);
                    if (operator === "BETWEEN") {
                        tableParams.ExpressionAttributeValues[ValueExpression + "_start"] = condition[0];
                        tableParams.ExpressionAttributeValues[ValueExpression + "_end"] = condition[1];
                        KeyConditionExpression[1] = keyName + " " + operator + " " + ValueExpression + "_start" + " AND " + ValueExpression + "_end";
                    } else {
                        tableParams.ExpressionAttributeValues[ValueExpression] = condition;
                        KeyConditionExpression[1] = keyName + " " + operator + " " + ValueExpression;
                    }
                } else if (condition && condition.constructor.name === "Array") {
                    tableParams.ExpressionAttributeValues[ValueExpression + "_start"] = condition[0];
                    tableParams.ExpressionAttributeValues[ValueExpression + "_end"] = condition[1];
                    KeyConditionExpression[1] = keyName + " BETWEEN " + ValueExpression + "_start" + " AND " + ValueExpression + "_end";
                } else {
                    tableParams.ExpressionAttributeValues[ValueExpression] = condition;
                    KeyConditionExpression[1] = keyName + " = " + ValueExpression;
                }

                if (globalKeys[key] && globalKeys[key].sort === key) {
                    tableParams.IndexName = globalKeys[key].IndexName;
                } else if (localKeys[key] && localKeys[key].sort === key) {
                    tableParams.IndexName = localKeys[key].IndexName;
                }
            } else {
                if (condition && condition.constructor.name === "Object") {

                    insideKey = Object.keys(condition)[0];
                    condition = condition[insideKey];
                    let operator = KeyOperatorLookup(insideKey);
                    if (operator === "BETWEEN") {
                        tableParams.ExpressionAttributeValues[ValueExpression + "_start"] = condition[0];
                        tableParams.ExpressionAttributeValues[ValueExpression + "_end"] = condition[1];
                        FilterExpression.push(keyName + " " + operator + " " + ValueExpression + "_start" + " AND " + ValueExpression + "_end");
                    } else if (operator === "IN") {
                        tableParams.ExpressionAttributeValues[ValueExpression] = "(" + condition.join(",") + ")";
                        FilterExpression.push(keyName + " " + operator + " " + ValueExpression);
                    } else {
                        tableParams.ExpressionAttributeValues[ValueExpression] = condition;
                        FilterExpression.push(keyName + " " + operator + " " + ValueExpression);
                    }
                } else if (condition && condition.constructor.name === "Array") { // SEES Its an array, 
                    
                    if (key.toUpperCase() === "AND") {
                        // TODO: HANDLE AND QUERY
                    } else if (key.toUpperCase() === "OR") {
                        let FilterExpressionValues = [];
                        condition.forEach( (key) => {
                            let keyName = "#" + Object.keys(key)[0].slice(0, 1).toUpperCase();
                            let ValueExpression = ":" + Object.keys(key)[0];
                    
                            if (tableParams.ExpressionAttributeNames[keyName] == undefined) {
                                tableParams.ExpressionAttributeNames[keyName] = Object.keys(key)[0];
                                tableParams.ExpressionAttributeValues[ValueExpression] = key[Object.keys(key)[0]];
                            } else {
                                let i = 1;
                                while (tableParams.ExpressionAttributeNames[keyName] != undefined) {
                                    keyName = "#" + key[Object.keys(key)[0]].slice(0, i);
                                    i++;
                                }
                                keyName = "#" + key[Object.keys(key)[0]].slice(0, i).toUpperCase();
                                tableParams.ExpressionAttributeNames[keyName] = Object.keys(key)[0];
                                tableParams.ExpressionAttributeValues[ValueExpression] = key[Object.keys(key)[0]];
                            }
                            
                            FilterExpressionValues.push(keyName + " = " + ValueExpression);
                            
                        });
                        let FilterExpressionString = FilterExpressionValues.join(" OR ");
                        FilterExpression.push(FilterExpressionString);
                        
                    } else {
                        tableParams.ExpressionAttributeValues[ValueExpression] = "(" + condition.join(",") + ")";
                        FilterExpression.push(keyName + " IN " + ValueExpression);
                    }
                } else {
                    tableParams.ExpressionAttributeValues[ValueExpression] = condition;
                    FilterExpression.push(keyName + " = " + ValueExpression);
                }
            }

        }
        
        tableParams.KeyConditionExpression = KeyConditionExpression.join(" AND ");
        if (countProperties(tableParams.ExpressionAttributeNames) > countProperties(KeyConditionExpression)) {
            //tableParams.FilterExpression = "";
            tableParams.FilterExpression = "" + FilterExpression.join(" AND ");
        }
        
    }
    queryString = queryString + " WITH QUERY OPERATION ";
    debug(queryString.blue, stopTimer(timeStart).bold.cyan);
    return tableParams;
}

/**
 * Builds table parameters for scan operation
 * @param  {[type]} model       Model object
 * @param  {[type]} filter      Filter
 * @param  {[type]} queryString String that holds query operation actions
 * @param  {[type]} timeStart   start time of operation
 */
function scan(model, filter, queryString, timeStart) {
    // Table parameters to do the query/scan
    var tableParams = {};
    // Define the filter if it does not exist
    if (!filter) {
        filter = {};
    }
    // Initialize query as an empty object
    var query = {};
    // Set scanfilter to empty object
    tableParams.ScanFilter = {};
    // If a where clause exists in the query, extract
    // the conditions from it.
    if (filter.where) {
        queryString = queryString + " WHERE ";
        for (var key in filter.where) {
            var condition = filter.where[key];
            // If condition is of type object, obtain key
            // and the actual condition on the key
            // In jugglingdb, `where` can have the following
            // forms.
            // 1) where : { key: value }
            // 2) where : { startTime : { gt : Date.now() } }
            // 3) where : { someKey : ["something","nothing"] }
            // condition now holds value in case 1),
            //  { gt: Date.now() } in case 2)
            // ["something, "nothing"] in case 3)
            var insideKey = null;
            if (condition && condition.constructor.name === "Object") {
                debug("Condition Type => Object \n\t Operator: %s\n\tCondition Value: %s", insideKey, condition);
                insideKey = Object.keys(condition)[0];
                condition = condition[insideKey];
                // insideKey now holds gt and condition now holds Date.now()
                query[key] = {
                    operator: OperatorLookup(insideKey),
                    attrs: condition
                };
            } else if (condition && condition.constructor.name === "Array") {
                debug("Condition Type => Array \n\t Operator: %s\n\tCondition Value: %s", insideKey, condition);
                query[key] = {
                    operator: "IN",
                    attrs: condition
                };
            } else {
                debug("Condition Type => Equality \n\tCondition Value: %s", condition);
                query[key] = {
                    operator: "EQ",
                    attrs: condition
                };
            }
            tableParams.ScanFilter[key] = {};
            tableParams.ScanFilter[key].ComparisonOperator = query[key].operator;
            tableParams.ScanFilter[key].AttributeValueList = [];

            var attrResult = query[key].attrs;
            //var attrResult = DynamoFromJSON(query[key].attrs);

            if (attrResult instanceof Array) {
                debug("Attribute Value list is an array");
                tableParams.ScanFilter[key].AttributeValueList = query[key].attrs;
                //tableParams.ScanFilter[key].AttributeValueList = DynamoFromJSON(query[key].attrs);
            } else {
                tableParams.ScanFilter[key].AttributeValueList.push(query[key].attrs);
                //tableParams.ScanFilter[key].AttributeValueList.push(DynamoFromJSON(query[key].attrs));
            }


            queryString = queryString + "`" + String(key) + "` " + String(query[key].operator) + " `" + String(query[key].attrs) + "`";
        }
    }
    queryString = queryString + " WITH SCAN OPERATION ";
    debug(queryString.blue, stopTimer(timeStart).bold.cyan);

    return tableParams;
}
/**
 *  Uses Amazon DynamoDB query/scan function to fetch all
 *  matching entries in the table.
 *
 */
DynamoDB.prototype.all = function all(modelName, filter, options, callback) {
    var timeStart = startTimer();
    var queryString = "GET ITEMS FROM TABLE ";
    // If limit is specified, use it to limit results
    var limitObjects;
    if (filter && filter.limit) {
        if (typeof(filter.limit) !== "number") {
            callback(new Error("Limit must be a number in Model.all function"), null);
            return;
        }
        limitObjects = filter.limit;
    }

    // Order, default by Sort Key. If Sort Key not present, order by Hashkey
    var orderByField;
    var args = {};
    const model = this.getModel(modelName);

    if (model.sortKey === undefined) {
        orderByField = model.hashKey;
        args[orderByField] = 1;
    } else {
        orderByField = model.sortKey;
        args[orderByField] = 1;
    }
    debug("orderByField: %s", orderByField);
    // Custom ordering
    if (filter && filter.order) {
        var keys = filter.order;
        if (typeof keys === "string") {
            keys = keys.split(",");
        }

        for (var index in keys) {
            var m = keys[index].match(/\s+(A|DE)SC$/);
            var keyA = keys[index];
            keyA = keyA.replace(/\s+(A|DE)SC$/, "").trim();
            orderByField = keyA;
            if (m && m[1] === "DE") {
                args[keyA] = -1;
            } else {
                args[keyA] = 1;
            }
        }

    }

    // Skip , Offset
    var offset;
    if (filter && filter.offset) {
        if (typeof(filter.offset) !== "number") {
            callback(new Error("Offset must be a number in Model.all function"), null);
            return;
        }
        offset = filter.offset;
    } else if (filter && filter.skip) {
        if (typeof(filter.skip) !== "number") {
            callback(new Error("Skip must be a number in Model.all function"), null);
            return;
        }
        offset = filter.skip;
    }

    queryString = queryString + String(this.tables(modelName));
    //debug("query string: %s", JSON.stringify(queryString));
    // If hashKey is present in where filter, use query
    var hashKeyFound = false;

    debug("Filter: %s ", JSON.stringify(filter));
    if (filter.where) {
        for (var key in filter.where) {
            debug("Key Search: %s => %s", key, model.hashKey);
            if (key === model.hashKey) {
                hashKeyFound = true;
                debug("Hash Key Found, QUERY operation will be used");
                break;
            }
        }
    }
    else if (filter) {
        filter.where = {}
        for (var key in filter) {
            debug("Key Search: %s => %s", key, model.hashKey);
            if (key === model.hashKey) {
                filter.where[key] = filter[key];
                hashKeyFound = true;
                debug("Hash Key Found, QUERY operation will be used");
                break;
            }
        }
    }

    // if (hashKeyFound === false) {
    //     callback(new Error("Hashkey Not Found."));
    // }

    // Check if an array of hash key values are provided. If yes, use scan.
    // Otherwise use query. This is because query does not support array of
    // hash key values
    // TODO: run multiple queries and return combined result
    if (hashKeyFound === true) {
        var condition = filter.where[model.hashKey];
        var insideKey = null;
        if ((condition && condition.constructor.name === "Object") || (condition && condition.constructor.name === "Array")) {
            insideKey = Object.keys(condition)[0];
            condition = condition[insideKey];
            if (condition instanceof Array) {
                hashKeyFound = false;
                debug("Hash key value is an array. Using SCAN operation instead");
            }
        }
    }

    // If true use query function
    if (hashKeyFound === true) {
        var tableParams = query(modelName, filter, model, queryString, timeStart);
        debug("tableParams: %s", JSON.stringify(tableParams));
        // Set table name based on model
        tableParams.TableName = this.tables(modelName);
        tableParams.ReturnConsumedCapacity = "TOTAL";

        var attributeSpecs = this._attributeSpecs[modelName];
        var LastEvaluatedKey = "junk";
        var queryResults = [];
        var finalResult = [];
        var hashKey = model.hashKey;
        var docClient = this.docClient;
        if (model.sortKey !== undefined) {
            var sortKey = model.sortKey;
        }

        tableParams.ExclusiveStartKey = undefined;
        // If KeyConditions exist, then call DynamoDB query function
        if (tableParams.KeyConditionExpression) {
            async.doWhilst(function(queryCallback) {

                debug("Query issued");

                docClient.query(tableParams, function(err, res) {
                    if (err || !res) {
                        queryCallback(err);
                    } else {
                        // Returns an array of objects. Pass each one to
                        // JSONFromDynamo and push to empty array
                        LastEvaluatedKey = res.LastEvaluatedKey;
                        if (LastEvaluatedKey !== undefined) {
                            debug("LastEvaluatedKey found. Refetching..");
                            tableParams.ExclusiveStartKey = LastEvaluatedKey;
                        }

                        queryResults = queryResults.concat(res.Items);
                        queryCallback();
                    }
                }.bind(this));
            }, function() {
                return LastEvaluatedKey !== undefined;
            }, function(err) {
                if (err) {
                    callback(err, null);
                } else {
                    if (offset !== undefined) {
                        debug("Offset by %s", offset);
                        queryResults = queryResults.slice(offset, limitObjects + offset);
                    }
                    if (limitObjects !== undefined) {
                        debug("Limit by %s", limitObjects);
                        queryResults = queryResults.slice(0, limitObjects);
                    }
                    debug("Sort by %s Order: %s", orderByField, args[orderByField] > 0 ? "ASC" : "DESC");
                    queryResults = helper.SortByKey(queryResults, orderByField, args[orderByField]);
                    if (filter && filter.include) {
                        debug("Model includes: %s", filter.include);
                        model.model.include(queryResults, filter.include, callback);
                    } else {
                        debug("Query results complete");
                        callback(null, queryResults);
                    }
                }
            }.bind(this));
        }

    } else {

      // Use scan function

      var tableParams = scan(modelName, filter, queryString, timeStart);
      debug("tableParams: %s", JSON.stringify(tableParams));
      // Set table name based on model
      tableParams.TableName = this.tables(modelName);
      tableParams.ReturnConsumedCapacity = "TOTAL";

      var attributeSpecs = this._attributeSpecs[modelName];
      var LastEvaluatedKey = "junk";
      var queryResults = [];
      var finalResult = [];
      var hashKey = model.hashKey;
      var docClient = this.docClient;
      if (model.sortKey !== undefined) {
        var sortKey = model.sortKey;
      }

      tableParams.ExclusiveStartKey = undefined;
      // If KeyConditions exist, then call DynamoDB query function
      debug("Scan issued");

      docClient.scan(tableParams, (err, res) => {
        if (err || !res) {
          queryCallback(err);
        } else {
          queryResults = res.Items;
          if (offset !== undefined) {
            debug("Offset by %s", offset);
            queryResults = queryResults.slice(offset, limitObjects + offset);
          }
          if (limitObjects !== undefined) {
            debug("Limit by %s", limitObjects);
            queryResults = queryResults.slice(0, limitObjects);
          }
          debug("Sort by %s Order: %s", orderByField, args[orderByField] > 0 ? "ASC" : "DESC");
          queryResults = helper.SortByKey(queryResults, orderByField, args[orderByField]);
          if (filter && filter.include) {
            debug("Model includes: %s", filter.include);
            model.model.include(queryResults, filter.include, callback);
          } else {
            debug("Query results complete");
            callback(null, queryResults);
          }
        }
      });
    }

};

/**
 * Save an object to the database
 * @param  {[type]}   modelName    [description]
 * @param  {[type]}   data     [description]
 * @param  {Function} callback [description]
 * @return {[type]}            [description]
 */
DynamoDB.prototype.save = function save(modelName, data, callback) {
    var timeStart = startTimer();
    var originalData = {};
    const model = this.getModel(modelName);
    var hashKey = model.hashKey;
    var sortKey = model.sortKey;
    var pkSeparator = model.pkSeparator;
    var pKey = model.pKey;

    /* Data is the original object coming in the body. In the body
       if the data has a key which is breakable, it must be chunked
       into N different attrs. N is specified by the breakValue[key]
    */
    var attributeSpecs = this._attributeSpecs[modelName];
    var outerCounter = 0;

    /*
      Checks for hash and sort keys
     */
    if ((data[hashKey] === null) || (data[hashKey] === undefined)) {
        var err = new Error("Hash Key `" + hashKey + "` cannot be null or undefined.");
        callback(err, null);
        return;
    }
    // If pKey is defined, sort key is also present.
    if (pKey !== undefined) {
        if ((data[sortKey] === null) || (data[sortKey] === undefined)) {
            var err = new Error("Sort Key `" + sortKey + "` cannot be null or undefined.");
            callback(err, null);
            return;
        } else {
            data[pKey] = String(data[hashKey]) + pkSeparator + String(data[sortKey]);
            originalData[pKey] = data[pKey];
        }
    }

    // Copy all attrs from data to originalData
    for (var key in data) {
        originalData[key] = data[key];
    }

    var queryString = "PUT ITEM IN TABLE ";
    var tableParams = {};
    tableParams.TableName = this.tables(modelName);
    tableParams.ReturnConsumedCapacity = "TOTAL";

    if (pKey !== undefined) {
        delete data[pKey];
    }
    tableParams.Item = data;
    this.docClient.put(tableParams, function(err, res) {
        if (err) {
            callback(err, null);
        } else {
            callback(null, originalData);
        }
    }.bind(this));

    debug(queryString.blue, stopTimer(timeStart).bold.cyan);
};


/**
 * Update all matching instances
 * @param {String} modelName The model name
 * @param {Object} where The search criteria
 * @param {Object} data The property/value pairs to be updated
 * @callback {Function} cb Callback function
 */
DynamoDB.prototype.update =
    DynamoDB.prototype.updateAll = function updateAll(modelName, where, data, options, cb) {
        var self = this;
        if (self.debug) {
            debug("updateAll", modelName, where, data, options);
        }
        var filter = {};
        filter.where = where;
        var idNames = this.idNames(modelName);
        var hashKey = model.hashKey;
        var sortKey = model.sortKey;

        //where = self.buildWhere(modelName, where);
        idNames.forEach((id) => delete data[id]);

        this.all(modelName, filter, options, function(err, results) {
            if (err || !results) {
                cb(err, null);
            } else {
                results.forEach((inst) => {
                    var instIds = {};
                    instIds[hashKey] = inst[hashKey];
                    instIds[sortKey] = inst[sortKey];
                    self.updateAttributes(modelName, instIds, data, options, cb);
                }); // loop through results and run updateattributes for each.
            }
        });
    };

/**
 * Update all matching instances
 * @param {String} modelName The model name
 * @param {Object} ids id values {hash: "value1", sort: "value2"}
 * @param {Object} data The property/value pairs to be updated
 * @param {Object} options loopback options object
 * @callback {Function} cb Callback function
 */
DynamoDB.prototype.patchAttributes =
    DynamoDB.prototype.updateAttributes = function(modelName, ids, data, options, callback) {
        var timeStart = startTimer();
        debug("updateAttributes(modelName, data, options, callback) using (%s, %s, %s, callback)", modelName, JSON.stringify(data), JSON.stringify(options));
        var originalData = {};
        const model = this.getModel(modelName);
        var hashKey = model.hashKey;
        var sortKey = model.sortKey;
        var tableParams = {};
        // Copy all attrs from data to originalData
        for (key in data) {
            originalData[key] = data[key];
        }

        var queryString = "UPDATE ITEM IN TABLE ";

        // Use updateItem function of DynamoDB

        // Set table name as usual
        tableParams.TableName = this.tables(modelName);
        tableParams.Key = {};
        tableParams.AttributeUpdates = {};
        tableParams.ReturnConsumedCapacity = "TOTAL";

        // Add hashKey / sortKey to tableParams
        debug(hashKey);
        debug(ids);
        tableParams.Key[hashKey] = ids[hashKey];
        if (sortKey !== undefined) {
            tableParams.Key[sortKey] = ids[sortKey];
        }

        debug("Tableparams:\n %s", JSON.stringify(tableParams));
        // Add attrs to update

        for (var key in data) {
            /*if (data[key] instanceof Date) {
              data[key] = Number(data[key]);
            }*/
            if (data.hasOwnProperty(key) && data[key] !== null && (key !== hashKey) && (key !== sortKey)) {
                tableParams.AttributeUpdates[key] = {};
                tableParams.AttributeUpdates[key].Action = "PUT";
                tableParams.AttributeUpdates[key].Value = data[key];
            }
        }
        tableParams.ReturnValues = "ALL_NEW";
        this.docClient.update(tableParams, function(err, res) {
            if (err) {
                callback(err, null);
            } else if (!res) {
                callback(null, null);
            } else {
                callback(null, res.data);
            }
        }.bind(this));
        debug(queryString.blue, stopTimer(timeStart).bold.cyan);
    };

DynamoDB.prototype.destroy = function(modelName, keys, callback) {
    var timeStart = startTimer();
    const model = this.getModel(modelName);
    var hashKey = model.hashKey;
    var sortKey = model.sortKey;

    if (sortKey !== undefined) {
        if (this._attributeSpecs[modelName][sortKey] === "number") {
            keys[sortKey] = parseInt(keys[sortKey]);
        } else if (this._attributeSpecs[modelName][sortKey] === "date") {
            keys[sortKey] = Number(keys[sortKey]);
        }
    }

    // If hashKey is of type Number use parseInt
    if (this._attributeSpecs[modelName][hashKey] === "number") {
        keys[hashKey] = parseInt(keys[hashKey]);
    } else if (this._attributeSpecs[modelName][hashKey] === "date") {
        keys[hashKey] = Number(keys[hashKey]);
    }

    // Use updateItem function of DynamoDB
    var tableParams = {};
    // Set table name as usual
    tableParams.TableName = this.tables(modelName);
    tableParams.Key = {};
    // Add hashKey to tableParams
    tableParams.Key[hashKey] = keys[hashKey];

    if (keys[sortKey] !== undefined) {
        tableParams.Key[hashKey] = keys[sortKey];
    }

    tableParams.ReturnValues = "ALL_OLD";
    var attributeSpecs = this._attributeSpecs[modelName];
    var outerCounter = 0;
    var chunkedData = {};

    this.docClient.delete(tableParams, function(err, res) {
        if (err) {
            callback(err, null);
        } else if (!res) {
            callback(null, null);
        } else {
            // Attributes is an object
            var tempString = "DELETE ITEM FROM TABLE " + tableParams.TableName + " WHERE " + hashKey + " `EQ` " + String(keys[hashKey]);
            debug(tempString.blue, stopTimer(timeStart).bold.cyan);
            callback(null, res.Attributes);
        }
    }.bind(this));

};

DynamoDB.prototype.defineForeignKey = function(modelName, key, cb) {
    var hashKey = this.getModel(modelName).hashKey;
    var attributeSpec = this._attributeSpecs[modelName].id || this._attributeSpecs[modelName][hashKey];
    if (attributeSpec === "string") {
        cb(null, String);
    } else if (attributeSpec === "number") {
        cb(null, Number);
    } else if (attributeSpec === "date") {
        cb(null, Date);
    }
};

/**
 * Destroy all deletes all records from table.
 * @param {Object} [where] Optional object that defines the criteria.  This is a "where" object. Do NOT pass a filter object.
 * @param {Object} [options] Options
 * @param  {Function} callback [description]
 */
DynamoDB.prototype.destroyAll = function(modelName, where, options, callback) {
    /*
      Note:
      Deleting individual items is extremely expensive. According to 
      AWS, a better solution is to destroy the table, and create it back again.
     */
    var timeStart = startTimer();
    var t = "DELETE EVERYTHING IN TABLE WHERE: " + this.tables(where);
    const model = this.getModel(modelName);
    var hashKey = model.hashKey;
    var sortKey = model.sortKey;

    var keys = {};
    var docClient = this.docClient;

    var self = this;
    var tableParams = {};
    tableParams.TableName = this.tables(modelName);
    docClient.scan(tableParams, function(err, res) {
        if (err) {
            callback(err);
            return;
        } else if (res === null) {
            callback(null);
            return;
        } else {
            async.mapSeries(res.Items, function(item, insideCallback) {

                if (sortKey === undefined) {
                    keys.hash = item[hashKey];
                } else {
                    keys.hash = item[hashKey];
                    keys.sort = item[sortKey];
                }
                self.destroy(modelName, keys, insideCallback);
            }, function(err, items) {
                if (err) {
                    callback(err);
                } else {
                    callback();
                }

            }.bind(this));
        }

    });
    debug(t.bold.red, stopTimer(timeStart).bold.cyan);
};

/**
 * Get number of records matching a filter
 * @param  {Object}   modelName
 * @param  {Function} callback 
 * @param  {Object}   where    : Filter
 * @return {Number}            : Number of matching records
 */
DynamoDB.prototype.count = function count(modelName, callback, where) {
    var filter = {};
    let options = {};
    filter.where = where;
    this.all(modelName, filter, options, function(err, results) {
        if (err || !results) {
            callback(err, null);
        } else {
            callback(null, results.length);
        }
    });
};


/**
 * Check if a given record exists
 * @param  {[type]}   modelName    [description]
 * @param  {[type]}   id       [description]
 * @param  {Function} callback [description]
 * @return {[type]}            [description]
 */
DynamoDB.prototype.exists = function exists(modelName, ids, callback) {
    let query = {};
    let options = {};
    query.where = {};
    Object.keys(ids).forEach(function(id) {
        query.where[id] = ids[id];
    });

    this.all(modelName, query, options, function(err, record) {
        if (err) {
            callback(err, null);
        } else if (isEmpty(record)) {
            callback(null, false);
        } else {
            callback(null, true);
        }
    });
};

DynamoDB.prototype.getModel = function getModel(name) {
  return this._models[name];
};
