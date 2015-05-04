/**
 * Module Dependencies
 */
var async = require('async'),
    _ = require('lodash'),
    db2 = require('ibm_db'),
    WaterlineAdapterErrors = require('waterline-errors').adapter;

/**
 * Sails Boilerplate Adapter
 *
 * Most of the methods below are optional.
 *
 * If you don't need / can't get to every method, just implement
 * what you have time for.  The other methods will only fail if
 * you try to call them!
 *
 * For many adapters, this file is all you need.  For very complex adapters, you may need more flexiblity.
 * In any case, it's probably a good idea to start with one file and refactor only if necessary.
 * If you do go that route, it's conventional in Node to create a `./lib` directory for your private submodules
 * and load them at the top of the file with other dependencies.  e.g. var update = `require('./lib/update')`;
 */
module.exports = (function () {
    var me = this;

    // You'll want to maintain a reference to each collection
    // (aka model) that gets registered with this adapter.
    me.connections = {};


    // You may also want to store additional, private data
    // per-collection (esp. if your data store uses persistent
    // connections).
    //
    // Keep in mind that models can be configured to use different databases
    // within the same app, at the same time.
    //
    // i.e. if you're writing a MariaDB adapter, you should be aware that one
    // model might be configured as `host="localhost"` and another might be using
    // `host="foo.com"` at the same time.  Same thing goes for user, database,
    // password, or any other config.
    //
    // You don't have to support this feature right off the bat in your
    // adapter, but it ought to get done eventually.
    //
    // Sounds annoying to deal with...
    // ...but it's not bad.  In each method, acquire a connection using the config
    // for the current model (looking it up from `_modelReferences`), establish
    // a connection, then tear it down before calling your method's callback.
    // Finally, as an optimization, you might use a db pool for each distinct
    // connection configuration, partioning pools for each separate configuration
    // for your adapter (i.e. worst case scenario is a pool for each model, best case
    // scenario is one single single pool.)  For many databases, any change to
    // host OR database OR user OR password = separate pool.
    me.dbPools = {};

    me.getConnectionString = function (connection) {
        var connectionData = [
            'DRIVER={DB2}',
            'DATABASE=' + connection.config.database,
            'HOSTNAME=' +  connection.config.host,
            'UID=' +  connection.config.user,
            'PWD=' +  connection.config.password,
            'PORT=' +  connection.config.port,
            'PROTOCOL=TCPIP'
        ];

        return connectionData.join(';');
    };

    me.escape = function (word) {
        return "'" + word.replace("'", "''") + "'";
    };

    // Data types
    // Waterline source: https://www.npmjs.org/package/waterline#attributes
    // IBM DB2 source: http://publib.boulder.ibm.com/infocenter/dzichelp/v2r2/index.jsp?topic=%2Fcom.ibm.db2z9.doc.sqlref%2Fsrc%2Ftpc%2Fdb2z_datatypesintro.htm
    me.typeMap = {
        // Times
        TIMESTMP: 'time',
        TIME: 'time',
        DATE: 'date',

        // Binaries
        BINARY: 'binary',
        VARBINARY: 'binary',

        // Strings
        CHAR: 'string',
        VARCHAR: 'string',
        GRAPHIC: 'string',
        VARGRAPHIC: 'string',

        // Integers
        SMALLINT: 'integer',
        INTEGER: 'integer',
        BIGINT: 'integer',

        // Floats
        DECIMAL: 'float',
        DECFLOAT: 'float',
        REAL: 'float',
        DOUBLE: 'float',

        // Texts
        CLOB: 'text',
        BLOB: 'text',
        DBCLOB: 'text',
        XML: 'text'
    };

    me.getSqlType = function (attrType) {
        var type = '';

        switch (attrType) {
            case 'string':
                type = 'VARCHAR';
                break;
            case 'integer':
                type = 'INTEGER';
                break;
            case 'float':
                type = 'DOUBLE';
                break;
            case 'text':
                type = 'BLOB';
                break;
            case 'binary':
                type = 'VARBINARY';
                break;
            case 'time':
                type = 'TIMESTMP';
                break;
            case 'date':
                type = 'DATE'
                break;
        }

        return type;
    };

    me.getSelectAttributes = function (collection) {
        return _.keys(collection.definition).join(',');
    };

    var adapter = {
        identity: 'sails-db2',

        // Set to true if this adapter supports (or requires) things like data types, validations, keys, etc.
        // If true, the schema for models using this adapter will be automatically synced when the server starts.
        // Not terribly relevant if your data store is not SQL/schemaful.
        syncable: true,


        // Default configuration for collections
        // (same effect as if these properties were included at the top level of the model definitions)
        defaults: {
            host: 'localhost',
            port: 50000,
            schema: true,
            ssl: false,

            // If setting syncable, you should consider the migrate option,
            // which allows you to set how the sync will be performed.
            // It can be overridden globally in an app (config/adapters.js)
            // and on a per-model basis.
            //
            // IMPORTANT:
            // `migrate` is not a production data migration solution!
            // In production, always use `migrate: safe`
            //
            // drop   => Drop schema and data, then recreate it
            // alter  => Drop/add columns as necessary.
            // safe   => Don't change anything (good for production DBs)
            migrate: 'alter'
        },


        /**
         *
         * This method runs when a model is initially registered
         * at server-start-time.  This is the only required method.
         *
         * @param  {[type]}   collection [description]
         * @param  {Function} cb         [description]
         * @return {[type]}              [description]
         */
        registerConnection: function (connection, collections, cb) {
            // Validate arguments
            if (!connection.identity) return cb(WaterlineAdapterErrors.IdentityMissing);
            if (me.connections[connection.identity]) return cb(WaterlineAdapterErrors.IdentityDuplicate);

            me.connections[connection.identity] = {
                config: connection,
                collections: collections,
                pool: connection.pool ? new db2.Pool() : null,
                conn: null
            };

            return cb();
        },


        /**
         * Fired when a model is unregistered, typically when the server
         * is killed. Useful for tearing-down remaining open connections,
         * etc.
         *
         * @param  {Function} cb [description]
         * @return {[type]}      [description]
         */
        teardown: function (connectionName, cb) {
            var closeConnection = function (connectionName) {
                var connection = me.connections[connectionName];
                if (connection.conn) connection.conn.close();

                delete me.connections[connectionName];
            };

            if (connectionName) closeConnection(connectionName);
            else _.each(me.connections, closeConnection);

            return cb();
        },


        /**
         *
         * REQUIRED method if integrating with a schemaful
         * (SQL-ish) database.
         *
         * @param  {[type]}   collectionName [description]
         * @param  {[type]}   definition     [description]
         * @param  {Function} cb             [description]
         * @return {[type]}                  [description]
         */
        define: function (connectionName, collectionName, definition, cb) {
            var connection = me.connections[connectionName],
                collection = connection.collections[collectionName],
                query = 'CREATE TABLE ' + collectionName,
                schemaData = [],
                schemaQuery = '';

            _.each(definition, function (attribute, attrName) {
                var attrType = me.getSqlType(attribute.type),
                    attrQuery = attrName;

                // @todo: handle unique and other DB2 data types
                if (attribute.primaryKey) {
                    if (attribute.autoIncrement) attrQuery += ' INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY';
                    else attrQuery += ' VARCHAR(255) NOT NULL PRIMARY KEY';
                }
                else {
                    switch (attrType) {
                        case 'VARCHAR':
                            var len = attribute.length || 255;
                            attrQuery += ' ' + attrType + '(' + len + ')';
                            break;
                        // @todo: handle each type with correct params
                        case 'DOUBLE':
                        case 'BLOB':
                        case 'VARBINARY':
                        case 'TIMESTMP':
                        case 'DATE':
                        case 'INTEGER':
                        default:
                            attrQuery += ' ' + attrType;
                    }
                }

                schemaData.push(attrQuery);
            });
            schemaQuery += '(' + schemaData.join(',') + ')';

            query += ' ' + schemaQuery;
            // @todo: use DB2 Database describe method instead of a SQL Query
            return adapter.query(connectionName, collectionName, query, function (err, result) {
                if (err) {
                    if (err.state !== '42S01') return cb(err);
                    result = [];
                }

                return cb(null, result);
            });
        },

        /**
         *
         * REQUIRED method if integrating with a schemaful
         * (SQL-ish) database.
         *
         * @param  {[type]}   collectionName [description]
         * @param  {Function} cb             [description]
         * @return {[type]}                  [description]
         */
        describe: function (connectionName, collectionName, cb) {
            var connection = me.connections[connectionName],
                collection = connection.collections[collectionName],
                query = 'SELECT DISTINCT(NAME), COLTYPE, IDENTITY, KEYSEQ, NULLS FROM Sysibm.syscolumns WHERE tbname = ' + me.escape(collectionName);

            // @todo: use DB2 Database describe method instead of a SQL Query
            adapter.query(connectionName, collectionName, query, function (err, attrs) {
                if (err) return cb(err);
                if (attrs.length === 0) return cb(null, null);

                var attributes = {};
                // Loop through Schema and attach extra attributes
                // @todo: check out a better solution to define primary keys following db2 docs
                attrs.forEach(function (attr) {
                    var attribute = {
                        type: me.typeMap[attr.COLTYPE.trim()]
                    };

                    if (attr.IDENTITY === 'Y' && attr.KEYSEQ !== 0 && attr.NULLS === 'N' && attribute.type === 'integer') {
                        attribute.primaryKey = true;
                        attribute.autoIncrement = true;
                        attribute.unique = true;
                    }

                    attributes[attr.NAME] = attribute;
                });

                cb(null, attributes);
            });
        },


        /**
         *
         *
         * REQUIRED method if integrating with a schemaful
         * (SQL-ish) database.
         *
         * @param  {[type]}   collectionName [description]
         * @param  {[type]}   relations      [description]
         * @param  {Function} cb             [description]
         * @return {[type]}                  [description]
         */
        drop: function (connectionName, collectionName, relations, cb) {
            if (_.isFunction(relations)) {
                cb = relations;
                relations = [];
            }

            var connection = me.connections[connectionName],
                connectionString = me.getConnectionString(connection),
                __DROP__ = function () {
                    // Drop any relations
                    var dropTable = function (tableName, next) {
                            // Build query
                            var query = 'DROP TABLE ' + tableName;

                            // Run query
                            connection.conn.query(query, next);
                        },
                        passCallback = function (err, result) {
                            if (err) {
                                if (err.state !== '42S02') return cb(err);
                                result = [];
                            }
                            cb(null, result);
                        };

                    async.eachSeries(relations, dropTable, function(err) {
                        if (err) return cb(err);

                        return dropTable(collectionName, passCallback);
                    });

                    connection.conn.query('DROP TABLE ' + collectionName, relations, passCallback);
                },
                operationCallback = function (err, conn) {
                    if (err) return cb(err);
                    else {
                        connection.conn = conn;
                        return __DROP__();
                    }
                };

            if (connection.pool) return connection.pool.open(connectionString, operationCallback);
            else return db2.open(connectionString, operationCallback);
        },


        // OVERRIDES NOT CURRENTLY FULLY SUPPORTED FOR:
        //
        // alter: function (collectionName, changes, cb) {},
        // addAttribute: function(collectionName, attrName, attrDef, cb) {},
        // removeAttribute: function(collectionName, attrName, attrDef, cb) {},
        // alterAttribute: function(collectionName, attrName, attrDef, cb) {},
        // addIndex: function(indexName, options, cb) {},
        // removeIndex: function(indexName, options, cb) {},

        query: function (connectionName, collectionName, query, data, cb) {
            if (_.isFunction(data)) {
                cb = data;
                data = null;
            }

            var connection = me.connections[connectionName],
                connectionString = me.getConnectionString(connection),
                __QUERY__ = function () {
                    var callback = function (err, records) {
                        if (err) cb(err);
                        else cb(null, records);
                    };

                    if (data) connection.conn.query(query, data, callback);
                    else connection.conn.query(query, callback);
                },
                operationCallback = function (err, conn) {
                    if (err) return cb(err);
                    else {
                        connection.conn = conn;
                        return __QUERY__();
                    }
                };

            if (connection.pool) return connection.pool.open(connectionString, operationCallback);
            else return db2.open(connectionString, operationCallback);
        },


        /**
         *
         * REQUIRED method if users expect to call Model.find(), Model.findOne(),
         * or related.
         *
         * You should implement this method to respond with an array of instances.
         * Waterline core will take care of supporting all the other different
         * find methods/usages.
         *
         * @param  {[type]}   collectionName [description]
         * @param  {[type]}   options        [description]
         * @param  {Function} cb             [description]
         * @return {[type]}                  [description]
         */
        find: function (connectionName, collectionName, options, cb) {
            var connection = me.connections[connectionName],
                collection = connection.collections[collectionName],
                connectionString = me.getConnectionString(connection),
                __FIND__ = function () {
                    var selectQuery = 'SELECT ' + me.getSelectAttributes(collection),
                        fromQuery = ' FROM ' + collection.tableName,
                        whereData = [],
                        whereQuery = '',
                        limitQuery = !_.isEmpty(options.limit) ? ' FETCH FIRST ' + options.limit + ' ROWS ONLY ' : '',
                        sortData = [],
                        sortQuery = '',
                        params = [],
                        sqlQuery = '';

                    // Building where clause
                    _.each(options.where, function (param, column) {
                        if (collection.definition.hasOwnProperty(column)) {
                            whereData.push(column + ' = ?');
                            params.push(param);
                        }
                    });
                    whereQuery += whereData.join(' AND ');
                    if (whereQuery.length > 0) whereQuery = ' WHERE ' + whereQuery;

                    // Building sort clause
                    _.each(options.sort, function (direction, column) {
                        if (collection.definition.hasOwnProperty(column)) {
                            //ORDER BY APPLICATIONCODE DESC

                            sortData.push(column + ' ' + direction);
                        }
                    });
                    sortQuery += sortData.join(', ');
                    if (sortQuery.length > 0) sortQuery = ' ORDER BY ' + sortQuery;

                    sqlQuery += selectQuery + fromQuery + whereQuery + sortQuery + limitQuery;
                    connection.conn.query(sqlQuery, params, cb);

                    // Options object is normalized for you:
                    //
                    // options.where
                    // options.limit
                    // options.sort

                    // Filter, paginate, and sort records from the datastore.
                    // You should end up w/ an array of objects as a result.
                    // If no matches were found, this will be an empty array.
                },
                operationCallback = function (err, conn) {
                    if (err) return cb(err);
                    else {
                        connection.conn = conn;
                        return __FIND__();
                    }
                };

            if (connection.pool) return connection.pool.open(connectionString, operationCallback);
            else return db2.open(connectionString, operationCallback);
        },

        /**
         *
         * REQUIRED method if users expect to call Model.create() or any methods
         *
         * @param  {[type]}   collectionName [description]
         * @param  {[type]}   values         [description]
         * @param  {Function} cb             [description]
         * @return {[type]}                  [description]
         */
        create: function (connectionName, collectionName, values, cb) {
            var connection = me.connections[connectionName],
                collection = connection.collections[collectionName],
                connectionString = me.getConnectionString(connection),
                __CREATE__ = function () {
                    var selectQuery = me.getSelectAttributes(collection),
                        columns = [],
                        params = [],
                        questions = [];

                    _.each(values, function (param, column) {
                        if (collection.definition.hasOwnProperty(column)) {
                            columns.push(column);
                            params.push(param);
                            questions.push('?');
                        }
                    });

                    connection.conn.query('SELECT ' + selectQuery + ' FROM FINAL TABLE (INSERT INTO ' + collection.tableName + ' (' + columns.join(',') + ') VALUES (' + questions.join(',') + '))', params, function (err, results) {
                        if (err) cb(err);
                        else cb(null, results[0]);
                    });
                },
                operationCallback = function (err, conn) {
                    if (err) return cb(err);
                    else {
                        connection.conn = conn;
                        return __CREATE__();
                    }
                };

            if (connection.pool) return connection.pool.open(connectionString, operationCallback);
            else return db2.open(connectionString, operationCallback);
        },

        /**
         *
         *
         * REQUIRED method if users expect to call Model.update()
         *
         * @param  {[type]}   collectionName [description]
         * @param  {[type]}   options        [description]
         * @param  {[type]}   values         [description]
         * @param  {Function} cb             [description]
         * @return {[type]}                  [description]
         */
        update: function (connectionName, collectionName, options, values, cb) {
            var connection = me.connections[connectionName],
                collection = connection.collections[collectionName],
                connectionString = me.getConnectionString(connection),
                __UPDATE__ = function () {
                    var selectQuery = me.getSelectAttributes(collection),
                        setData = [],
                        setQuery = '',
                        whereData = [],
                        whereQuery = '',
                        params = [],
                        sqlQuery = '';

                    _.each(values, function (param, column) {
                        if (collection.definition.hasOwnProperty(column) && !collection.definition[column].autoIncrement) {
                            setData.push(column + ' = ?');
                            params.push(param);
                        }
                    });
                    setQuery = ' SET ' + setData.join(',');

                    _.each(options.where, function (param, column) {
                        if (collection.definition.hasOwnProperty(column)) {
                            whereData.push(column + ' = ?');
                            params.push(param);
                        }
                    });
                    whereQuery += whereData.join(' AND ');

                    if (whereQuery.length > 0) whereQuery = ' WHERE ' + whereQuery;

                    sqlQuery = 'SELECT ' + selectQuery + ' FROM FINAL TABLE (UPDATE ' + collection.tableName + setQuery + whereQuery + ')';

                    connection.conn.query(sqlQuery, params, function (err, results) {
                        if (err) cb(err);
                        else cb(null, results[0]);
                    });
                },
                operationCallback = function (err, conn) {
                    if (err) return cb(err);
                    else {
                        connection.conn = conn;
                        return __UPDATE__();
                    }
                };

            if (connection.pool) return connection.pool.open(connectionString, operationCallback);
            else return db2.open(connectionString, operationCallback);
        },

        /**
         *
         * REQUIRED method if users expect to call Model.destroy()
         *
         * @param  {[type]}   collectionName [description]
         * @param  {[type]}   options        [description]
         * @param  {Function} cb             [description]
         * @return {[type]}                  [description]
         */
        destroy: function (connectionName, collectionName, options, cb) {
            var connection = me.connections[connectionName],
                collection = connection.collections[collectionName],
                connectionString = me.getConnectionString(connection),
                __DESTROY__ = function () {
                    var whereData = [],
                        whereQuery = '',
                        params = [];

                    _.each(options.where, function (param, column) {
                        if (collection.definition.hasOwnProperty(column)) {
                            whereData.push(column + ' = ?');
                            params.push(param);
                        }
                    });
                    whereQuery += whereData.join(' AND ');

                    if (whereQuery.length > 0) whereQuery = ' WHERE ' + whereQuery;

                    connection.conn.query('DELETE FROM ' + collection.tableName + whereQuery, params, cb);
                },
                operationCallback = function (err, conn) {
                    if (err) return cb(err);
                    else {
                        connection.conn = conn;
                        return __DESTROY__();
                    }
                };

            if (connection.pool) return connection.pool.open(connectionString, operationCallback);
            else return db2.open(connectionString, operationCallback);
        }


        /*
         **********************************************
         * Optional overrides
         **********************************************

         // Optional override of built-in batch create logic for increased efficiency
         // (since most databases include optimizations for pooled queries, at least intra-connection)
         // otherwise, Waterline core uses create()
         createEach: function (collectionName, arrayOfObjects, cb) { cb(); },

         // Optional override of built-in findOrCreate logic for increased efficiency
         // (since most databases include optimizations for pooled queries, at least intra-connection)
         // otherwise, uses find() and create()
         findOrCreate: function (collectionName, arrayOfAttributeNamesWeCareAbout, newAttributesObj, cb) { cb(); },
         */


        /*
         **********************************************
         * Custom methods
         **********************************************

         ////////////////////////////////////////////////////////////////////////////////////////////////////
         //
         // > NOTE:  There are a few gotchas here you should be aware of.
         //
         //    + The collectionName argument is always prepended as the first argument.
         //      This is so you can know which model is requesting the adapter.
         //
         //    + All adapter functions are asynchronous, even the completely custom ones,
         //      and they must always include a callback as the final argument.
         //      The first argument of callbacks is always an error object.
         //      For core CRUD methods, Waterline will add support for .done()/promise usage.
         //
         //    + The function signature for all CUSTOM adapter methods below must be:
         //      `function (collectionName, options, cb) { ... }`
         //
         ////////////////////////////////////////////////////////////////////////////////////////////////////


         // Custom methods defined here will be available on all models
         // which are hooked up to this adapter:
         //
         // e.g.:
         //
         foo: function (collectionName, options, cb) {
         return cb(null,"ok");
         },
         bar: function (collectionName, options, cb) {
         if (!options.jello) return cb("Failure!");
         else return cb();
         }

         // So if you have three models:
         // Tiger, Sparrow, and User
         // 2 of which (Tiger and Sparrow) implement this custom adapter,
         // then you'll be able to access:
         //
         // Tiger.foo(...)
         // Tiger.bar(...)
         // Sparrow.foo(...)
         // Sparrow.bar(...)


         // Example success usage:
         //
         // (notice how the first argument goes away:)
         Tiger.foo({}, function (err, result) {
         if (err) return console.error(err);
         else console.log(result);

         // outputs: ok
         });

         // Example error usage:
         //
         // (notice how the first argument goes away:)
         Sparrow.bar({test: 'yes'}, function (err, result){
         if (err) console.error(err);
         else console.log(result);

         // outputs: Failure!
         })




         */


    };


    // Expose adapter definition
    return adapter;
})();