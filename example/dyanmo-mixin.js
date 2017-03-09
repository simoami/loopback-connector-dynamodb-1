var debug = require("debug")("strong-remoting:remotes");
module.exports = function(DynamoModel, options) {
  // Manually define key names after generating a DynamoModel

    var hashKeyName = options.hk;
    var sortKeyName = options.sk;


    DynamoModel.getById = function(hashkey, sortkey, filter, cb) {
        var query = {};
        query.where = {};
        if (filter !== undefined) {
            query = Object.assign(query, filter);
        }
        query.where[hashKeyName] = hashkey;

        if (sortkey === undefined && query.where[sortKeyName] === undefined) {
            DynamoModel.find(query, function(err, DynamoModels) {
                debug(DynamoModels);
                return cb(err, DynamoModels);
            });
        } else {
            query.where[sortKeyName] = sortkey;
            DynamoModel.findOne(query, function(err, DynamoModels) {
                return cb(err, DynamoModels);
            });
        }
    };

    DynamoModel.existsDynamo = function(hashkey, sortkey, cb) {
        var ids = {};
        ids[hashKeyName] = hashkey;
        if (sortkey === undefined) {
            DynamoModel.exists(ids, function(err, DynamoModels) {
                return cb(err, DynamoModels);
            });
        }
        ids[sortKeyName] = sortkey;
        DynamoModel.exists(ids, function(err, DynamoModels) {
            return cb(err, DynamoModels);
        });
    };

  /**
   * Patch attributes of a DynamoModel instance and persist to DB
   * @param {string} hashkey Hash key of DynamoModel instance
   * @param {string} sortkey Sort key of DynamoModel instance
   * @param {object} data An object of DynamoModel property name/value pairs
   * @param {Function(Error, object)} callback
   */

    DynamoModel.patchDynamo = function(hashkey, sortkey, data, callback) {
        var query = {};
        query[hashKeyName] = hashkey;
        if ((sortkey !== undefined) && ((data[sortKeyName] === undefined) || (data[sortKeyName] === sortkey))) {
            query[sortKeyName] = sortkey;
        } else {
            throw new Error("sortkey does not match request");
        }

        if ((hashkey !== undefined) && ((data[hashKeyName] === undefined) || (data[hashKeyName] === hashkey))) {
            query[hashKeyName] = hashkey;
        } else {
            throw new Error("hashkey does not match request");
        }

        DynamoModel.upsert(data, query, function(err, DynamoModels) {

            return callback(err, DynamoModels);
        });
    };



    DynamoModel.remoteMethod("patchDynamo", {
      
        description: "Patch attributes of a DynamoModel instance and persist to the data source ",
        http: [{
            path: "/:zipcode/:sortkey",
            verb: "patch",
        }, {
            path: "/:hashkey/:sortkey",
            verb: "put",
        }
    /*, {
      path: "/",
      verb: "patch",
    }, {
      path: "/",
      verb: "put",
        }*/],

        accepts: [{
            arg: "zipcode",
            type: "string",
            required: true,
            description: "Hash key of DynamoModel instance",
            http: {
                source: "path",
            },
        },
        {
            arg: "sortkey",
            type: "string",
            required: true,
            description: "Sort key of DynamoModel instance",
            http: {
                source: "path",
            },
        },
        {
            arg: "data",
            type: "Object",
            required: true,
            description: "An object of DynamoModel property name/value pairs",
            http: {
                source: "body",
            },
        },
        ],
        returns: [{
            arg: "result",
            type: "Object",
            root: true,
            description: "return full body",
        }],
    });

 
    DynamoModel.deleteByIdDynamo = function(hashkey, sortkey, callback) {
        var ids = {};
        debug("hashkey: %s", hashkey);
        debug("sortkey: %s", sortkey);
        ids[hashKeyName] = hashkey;
        ids[sortKeyName] = sortkey;    
        DynamoModel.destroy(ids, options, function(err, DynamoModels) {
            return callback(err, DynamoModels);
        });
    };


    DynamoModel.upsertWithWhereDynamo = function(hashkey, where, data, callback) {
        var opts = {};
        if (where === undefined) {
            throw new Error("Where cannot be empty");
        }
        if ((hashKeyName in data) && ((where[hashKeyName] !== hashkey) || (data[hashKeyName] !== hashkey))) {
            throw new Error("hashkey does not match request");
        }
        debug("DATA: %s", JSON.stringify(data));
        where = JSON.parse(where);
        debug("hashkey: %s", hashkey);
        if (hashkey === undefined) {
            if (hashKeyName in where) {
                hashkey = where[hashKeyName];
            } else {
                throw new Error("hashkey not defined or found in where");
            }
        }
        where[hashKeyName] = hashkey;

        DynamoModel.upsertWithWhere(where, data, opts, function(err, DynamoModels) {
            return callback(err, DynamoModels);
        });
    };

    DynamoModel.remoteMethod("upsertWithWhereDynamo", {
        description: "Patch attributes of a DynamoModel instance and persist to the data source ",
        http: [{
            path: "/:hashkey/upsertWithWhere",
            verb: "post",
        }],

        accepts: [{
            arg: "hashkey",
            type: "string",
            required: true,
            description: "Hash key of DynamoModel instance",
            http: {
                source: "path",
            },
        },
        {
            arg: "where",
            type: "string",
            description: "where filter",
            http: {
                source: "query",
            },
        },
        {
            arg: "data",
            type: "Object",
            description: "An object of a Dynamo Model property name/value pairs",
            http: {
                source: "body",
            },
        },
        ],
        returns: [{
            arg: "result",
            type: "Object",
            root: true,
            description: "return full body",
        }],
    });


    DynamoModel.remoteMethod("deleteByIdDynamo", {
        description: "Patch attributes of a DynamoModel instance and persist to the data source ",
        http: [{
            path: "/:hashkey/:sortkey",
            verb: "delete",
        }],

        accepts: [{
            arg: "hashkey",
            type: "string",
            required: true,
            description: "Hash key of DynamoModel instance",
            http: {
                source: "path",
            },
        },
        {
            arg: "sortkey",
            type: "string",
            required: true,
            description: "Sort key of DynamoModel instance",
            http: {
                source: "path",
            },
        },
        {
            arg: "opts",
            type: "string",
            description: "options",
            http: {
                source: "any",
            },
        }
        ],
        returns: [{
            arg: "result",
            type: "Object",
            root: true,
            description: "return full body",
        }],
    });


    DynamoModel.remoteMethod("count", {
        description: "Patch attributes of a DynamoModel instance and persist to the data source ",
        http: [{
            path: "/:hashkey/count",
            verb: "get",
        }],

        accepts: [{
            arg: "hashkey",
            type: "string",
            required: true,
            description: "Hash key of DynamoModel instance",
            http: {
                source: "path",
            },
        },
        {
            arg: "where",
            type: "string",
            description: "where filter",
            http: {
                source: "query",
            },
        },
        ],
        returns: [{
            arg: "result",
            type: "Object",
            root: true,
            description: "return full body",
        }],
    });

    DynamoModel.remoteMethod("existsDynamo", {
        description: "Check whether a model instance exists in the data source",
        http: [{
            path: "/:hashkey/:sortkey/exists",
            verb: "get",
        }, {
            path: "/:hashkey/:sortkey",
            verb: "head",
        }],
        accepts: [{
            arg: "hashkey",
            type: "string",
            required: true,
            description: "Hash key of DynamoModel instance",
            http: {
                source: "path",
            },
        },
        {
            arg: "sortkey",
            type: "string",
            required: true,
            description: "Sort key of DynamoModel instance",
            http: {
                source: "path",
            },
        },
        ],
        returns: [{
            arg: "result",
            type: "Object",
            root: true,
            description: "return full body",
        }]
    });
};
