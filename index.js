/**
 * Module Dependencies
 */
var _ = require('lodash'),
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

    me.decorate = function (operation) {
        return function (connectionName, collectionName, options, data, cb) {
            var connection = me.connections[connectionName],
                collection = connection.collections[collectionName],
                connectionData = [
                    'DATABASE=' + connection.config.database,
                    'HOSTNAME=' +  connection.config.host,
                    'UID=' +  connection.config.username,
                    'PWD=' +  connection.config.password,
                    'PORT=' +  connection.config.port,
                    'PROTOCOL=TCPIP'
                ],
                connectionString = connectionData.join(';'),
                operationCallback = function (err, conn) {
                    if (err) return cb(err);
                    else return operation(conn, collection, options, data, cb);
                };

            if (connection.pool) return connection.pool.open(connectionString, operationCallback);
            else return db2.open(connectionString, operationCallback);
        };
    };


    var adapter = {
        identity: 'sails-db2',

        // Set to true if this adapter supports (or requires) things like data types, validations, keys, etc.
        // If true, the schema for models using this adapter will be automatically synced when the server starts.
        // Not terribly relevant if your data store is not SQL/schemaful.
        syncable: false,


        // Default configuration for collections
        // (same effect as if these properties were included at the top level of the model definitions)
        defaults: {

            // For example:
            // port: 3306,
            // host: 'localhost',
            // schema: true,
            // ssl: false,
            // customThings: ['eh']

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
                pool: connection.pool ? new db2.Pool() : null
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
        teardown: function (cb) {
            cb();
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
        /*define: function (collectionName, definition, cb) {

            // If you need to access your private data for this collection:
            var collection = _modelReferences[collectionName];

            // Define a new "table" or "collection" schema in the data store
            cb();
        },*/

        /**
         *
         * REQUIRED method if integrating with a schemaful
         * (SQL-ish) database.
         *
         * @param  {[type]}   collectionName [description]
         * @param  {Function} cb             [description]
         * @return {[type]}                  [description]
         */
        /*describe: function (collectionName, cb) {

            // If you need to access your private data for this collection:
            var collection = _modelReferences[collectionName];

            // Respond with the schema (attributes) for a collection or table in the data store
            var attributes = {};
            cb(null, attributes);
        },*/


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
        drop: function (collectionName, relations, cb) {
            // If you need to access your private data for this collection:
            var collection = _modelReferences[collectionName];

            // Drop a "table" or "collection" schema from the data store
            cb();
        },


        // OVERRIDES NOT CURRENTLY FULLY SUPPORTED FOR:
        //
        // alter: function (collectionName, changes, cb) {},
        // addAttribute: function(collectionName, attrName, attrDef, cb) {},
        // removeAttribute: function(collectionName, attrName, attrDef, cb) {},
        // alterAttribute: function(collectionName, attrName, attrDef, cb) {},
        // addIndex: function(indexName, options, cb) {},
        // removeIndex: function(indexName, options, cb) {},


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
        find: me.decorate(function (connection, collection, options, cb) {
            'SELECT ASD, LOL FROM TABLENAME WHERE ASD=LOL AND LOL=ASD';

            var selectData = _.keys(collection.schema),
                selectQuery = selectData.join(','),
                whereData = [],
                whereQuery = '',
                params = [];

            _.each(options.where, function (key, value) {
                whereData.push(key + ' = ?');
                params.push(value);
            });
            whereQuery = whereData.join(' AND ');

            connection.query('SELECT ' + selectQuery + ' FROM ' + collection.tableName + ' WHERE ' + whereQuery, params, function (err, records) {
                if (err) cb(err);
                else cb(null, records);
            });



            // Options object is normalized for you:
            //
            // options.where
            // options.limit
            // options.skip
            // options.sort

            // Filter, paginate, and sort records from the datastore.
            // You should end up w/ an array of objects as a result.
            // If no matches were found, this will be an empty array.

            // Respond with an error, or the results.
            cb(null, []);
        }),

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
                connectionData = [
                        'DATABASE=' + connection.config.database,
                        'HOSTNAME=' +  connection.config.host,
                        'UID=' +  connection.config.username,
                        'PWD=' +  connection.config.password,
                        'PORT=' +  connection.config.port,
                    'PROTOCOL=TCPIP'
                ],
                connectionString = connectionData.join(';'),
                operation = function (connection, collection, values, cb) {
                    var columns = [],
                        params = [],
                        questions = [];

                    _.each(values, function (key, value) {
                        columns.push(key);
                        params.push(value);
                        questions.push('?');
                    });

                    connection.query('INSERT INTO ' + collection.tableName + ' (' + columns.join(',') + ') VALUES (' + questions.join(',') + ')', params, function (err, record) {
                        if (err) cb(err);
                        else cb(null, record);
                    });
                },
                operationCallback = function (err, conn) {
                    if (err) return cb(err);
                    else return operation(conn, collection, values, cb);
                };

            if (connection.pool) return connection.pool.open(connectionString, operationCallback);
            else return db2.open(connectionString, operationCallback);
        },


        //

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
        update: me.decorate(function (connection, collection, options, values, cb) {
            'UPDATE TABLE_NAME SET COL1=VAL1 WHERE ID = ID AND bla = bla';
            var setData = [],
                setQuery = '',
                whereData = [],
                whereQuery = '',
                params = [];

            _.each(values, function (key, value) {
                setData.push(key + ' = ?');
                params.push(value);
            });
            setQuery = setData.join(',');

            _.each(options.where, function (key, value) {
                whereData.push(key + ' = ?');
                params.push(value);
            });
            whereQuery = whereData.join(' AND ');

            connection.query('UPDATE ' + collection.tableName + ' SET ' + setQuery + ' WHERE ' + whereQuery, params, function (err, record) {
                if (err) cb(err);
                else cb(null, record);
            });
        }),

        /**
         *
         * REQUIRED method if users expect to call Model.destroy()
         *
         * @param  {[type]}   collectionName [description]
         * @param  {[type]}   options        [description]
         * @param  {Function} cb             [description]
         * @return {[type]}                  [description]
         */
        destroy: me.decorate(function (connection, collection, options, cb) {
            'DELETE FROM TABLENAME WHERE ASD=LOL AND LOL=ASD';
            var whereData = [],
                whereQuery = '',
                params = [];

            _.each(options.where, function (key, value) {
                whereData.push(key + ' = ?');
                params.push(value);
            });
            whereQuery = whereData.join(' AND ');

            connection.query('DELETE FROM ' + collection.tableName + ' WHERE ' + whereQuery, params, function (err, record) {
                if (err) cb(err);
                else cb(null, record);
            });
        })


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