/*---------------------------------------------------------------
  :: sails-db2
  -> adapter
---------------------------------------------------------------*/

// Dependencies
var 
    //pg = require('pg'),
    _ = require('lodash'),
    async = require('async'),
    Query = require('./query'),
    utils = require('./utils'),
    Errors = require('waterline-errors').adapter;

module.exports = (function() {

  // Keep track of all the connections used by the app
  var connections = {};

  var adapter = {
    identity: 'sails-db2',

    // Which type of primary key is used by default
    pkFormat: 'integer',

    syncable: true,

    defaults: {
      host: 'localhost',
      port: 5432,
      schema: true,
      ssl: false
    },

    /*************************************************************************/
    /* Public Methods for Sails/Waterline Adapter Compatibility              */
    /*************************************************************************/

    // Register a new DB Connection
    registerConnection: function(connection, collections, cb) {

      var self = this;

      if(!connection.identity) return cb(Errors.IdentityMissing);
      if(connections[connection.identity]) return cb(Errors.IdentityDuplicate);

      // Store the connection
      connections[connection.identity] = {
        config: connection,
        collections: collections
      };

      // Always call describe
      async.map(Object.keys(collections), function(colName, cb){
        self.describe(connection.identity, colName, cb)
      }, cb);
    },

    // Teardown
    teardown: function(connectionName, cb) {
      if(!connections[connectionName]) return cb();
      delete connections[connectionName];
      cb();
    },

    // Raw Query Interface
    query: function(connectionName, table, query, data, cb) {

      if (_.isFunction(data)) {
        cb = data;
        data = null;
      }

      spawnConnection(connectionName, function __QUERY__(client, cb) {

        // Run query
        if (data) client.query(query, data, cb);
        else client.query(query, cb);

      }, cb);
    },

    // Describe a table
    describe: function(connectionName, collectionName, cb) {

      spawnConnection(connectionName, function __DESCRIBE__(connection, cb) {

        var connectionObject = connections[connectionName];
        var collection = connectionObject.collections[collectionName];
        if (!collection) {
          return cb(util.format('Unknown collection `%s` in connection `%s`', collectionName, connectionName));
        }
        var tableName = collectionName;

        var query = 'DESCRIBE ' + tableName;
        var pkQuery = 'DESCRIBE INDEXES FOR ' + tableName + ' SHOW DETAIL;';

        connection.query(query, function __DESCRIBE__(err, schema) {
          if (err) {
            if (err.code === 'ER_NO_SUCH_TABLE') {
              return cb();
            } else return cb(err);
          }

          connection.query(pkQuery, function(err, pkResult) {
            if(err) return cb(err);

            // Loop through Schema and attach extra attributes
            schema.forEach(function(attr) {

              // Set Primary Key Attribute
              if(attr.Key === 'PRI') {
                attr.primaryKey = true;

                // If also an integer set auto increment attribute
                if(attr.Type === 'int(11)') {
                  attr.autoIncrement = true;
                }
              }

              // Set Unique Attribute
              if(attr.Key === 'UNI') {
                attr.unique = true;
              }
            });

            // Loop Through Indexes and Add Properties
            pkResult.forEach(function(result) {
              schema.forEach(function(attr) {
                if(attr.Field !== result.Column_name) return;
                attr.indexed = true;
              });
            });

            // TODO: check that what was returned actually matches the cache
            cb(null, schema);
          });

        });
      }, cb);
    },

    // Create a new table
    define: function(connectionName, table, definition, cb) {

      // Create a describe method to run after the define.
      // Ensures the define connection is properly closed.
      var describe = function(err, result) {
        if(err) return cb(err);

        // Describe (sets schema)
        //adapter.describe(connectionName, table.replace(/["']/g, ""), cb);

        console.dir(result);
      };

      spawnConnection(connectionName, function __DEFINE__(client, cb) {

        // Escape Table Name
        table = utils.escapeName(table);

        // Iterate through each attribute, building a query string
        var _schema = utils.buildSchema(definition);

        // Check for any Index attributes
        var indexes = utils.buildIndexes(definition);

        // Build Query
        var query = 'CREATE TABLE ' + table + ' (' + _schema + ')';

        // Run Query
        // client.query(query, function __DEFINE__(err, result) {
        //   if(err) return cb(err);

          // Build Indexes
          // function buildIndex(name, cb) {

          //   // Strip slashes from table name, used to namespace index
          //   var cleanTable = table.replace(/['"]/g, '');

          //   // Build a query to create a namespaced index tableName_key
          //   var query = 'CREATE INDEX ' + cleanTable + '_' + name + ' on ' + table + ' (' + name + ');';

          //   // Run Query
          //   client.query(query, function(err, result) {
          //     if(err) return cb(err);
          //     cb();
          //   });
          // }

          // Build indexes in series
        //   async.eachSeries(indexes, buildIndex, cb);
        // });

      }, describe);
    },

    // // Drop a table
    // drop: function(connectionName, table, relations, cb) {

    //   if(typeof relations === 'function') {
    //     cb = relations;
    //     relations = [];
    //   }

    //   spawnConnection(connectionName, function __DROP__(client, cb) {

    //     // Drop any relations
    //     function dropTable(item, next) {

    //       // Build Query
    //       var query = 'DROP TABLE ' + utils.escapeName(item) + ';';

    //       // Run Query
    //       client.query(query, function __DROP__(err, result) {
    //         if(err) result = null;
    //         next(null, result);
    //       });
    //     }

    //     async.eachSeries(relations, dropTable, function(err) {
    //       if(err) return cb(err);
    //       dropTable(table, cb);
    //     });

    //   }, cb);
    // },

    // // Add a column to a table
    // addAttribute: function(connectionName, table, attrName, attrDef, cb) {
    //   spawnConnection(connectionName, function __ADD_ATTRIBUTE__(client, cb) {

    //     // Escape Table Name
    //     table = utils.escapeName(table);

    //     // Setup a Schema Definition
    //     var attrs = {};
    //     attrs[attrName] = attrDef;

    //     var _schema = utils.buildSchema(attrs);

    //     // Build Query
    //     var query = 'ALTER TABLE ' + table + ' ADD COLUMN ' + _schema;

    //     // Run Query
    //     client.query(query, function __ADD_ATTRIBUTE__(err, result) {
    //       if(err) return cb(err);
    //       cb(null, result.rows);
    //     });

    //   }, cb);
    // },

    // // Remove a column from a table
    // removeAttribute: function (connectionName, table, attrName, cb) {
    //   spawnConnection(connectionName, function __REMOVE_ATTRIBUTE__(client, cb) {

    //     // Escape Table Name
    //     table = utils.escapeName(table);

    //     // Build Query
    //     var query = 'ALTER TABLE ' + table + ' DROP COLUMN "' + attrName + '" RESTRICT';

    //     // Run Query
    //     client.query(query, function __REMOVE_ATTRIBUTE__(err, result) {
    //       if(err) return cb(err);
    //       cb(null, result.rows);
    //     });

    //   }, cb);
    // },

    // // Add a new row to the table
    // create: function(connectionName, table, data, cb) {
    //   spawnConnection(connectionName, function __CREATE__(client, cb) {

    //     var connectionObject = connections[connectionName];
    //     var collection = connectionObject.collections[table];

    //     var schemaName = collection.meta && collection.meta.schemaName
    //                       ? utils.escapeName(collection.meta.schemaName) + '.'
    //                       : '';
    //     var tableName = schemaName + utils.escapeName(table);

    //     // Build a Query Object
    //     var _query = new Query(collection.definition);

    //     // Cache the original table name for later use
    //     var originalTable = _.clone(table);

    //     // Transform the Data object into arrays used in a parameterized query
    //     var attributes = utils.mapAttributes(data),
    //         columnNames = attributes.keys.join(', '),
    //         paramValues = attributes.params.join(', ');

    //     var incrementSequences = [];

    //     // Loop through all the attributes being inserted and check if a sequence was used
    //     Object.keys(collection.schema).forEach(function(schemaKey) {
    //       if(!utils.object.hasOwnProperty(collection.schema[schemaKey], 'autoIncrement')) return;
    //       if(Object.keys(data).indexOf(schemaKey) < 0) return;
    //       incrementSequences.push(schemaKey);
    //     });

    //     // Build Query
    //     var query = 'INSERT INTO ' + tableName + ' (' + columnNames + ') values (' + paramValues + ') RETURNING *';

    //     // Run Query
    //     client.query(query, attributes.values, function __CREATE__(err, result) {
    //       if(err) return cb(err);

    //       // Cast special values
    //       var values = _query.cast(result.rows[0]);

    //       // Set Sequence value to defined value if needed
    //       if(incrementSequences.length === 0) return cb(null, values);

    //       function setSequence(item, next) {
    //         var sequenceName = "'" + originalTable + '_' + item + '_seq' + "'";
    //         var sequenceValue = values[item];
    //         var sequenceQuery = 'SELECT setval(' + sequenceName + ', ' + sequenceValue + ', true)';

    //         client.query(sequenceQuery, function(err, result) {
    //           if(err) return next(err);
    //           next();
    //         });
    //       }

    //       async.each(incrementSequences, setSequence, function(err) {
    //         if(err) return cb(err);
    //         cb(null, values);
    //       });

    //     });

    //   }, cb);
    // },

    // // Add a multiple rows to the table
    // createEach: function(connectionName, table, records, cb) {
    //   spawnConnection(connectionName, function __CREATE_EACH__(client, cb) {

    //     var connectionObject = connections[connectionName];
    //     var collection = connectionObject.collections[table];

    //     var schemaName = collection.meta && collection.meta.schemaName
    //                       ? utils.escapeName(collection.meta.schemaName) + '.'
    //                       : '';
    //     var tableName = schemaName + utils.escapeName(table);

    //     // Build a Query Object
    //     var _query = new Query(collection.definition);

    //     // Collect Query Results
    //     var results = [];

    //     // Simple way for now, in the future make this more awesome
    //     async.each(records, function(data, cb) {

    //       // Transform the data object into arrays for parameterized query
    //       var attributes = utils.mapAttributes(data),
    //           columnNames = attributes.keys.join(', '),
    //           paramValues = attributes.params.join(', ');

    //       // Build Query
    //       var query = 'INSERT INTO ' + tableName + ' (' + columnNames +
    //         ') values (' + paramValues + ') RETURNING *;';

    //       // Run Query
    //       client.query(query, attributes.values, function __CREATE_EACH__(err, result) {
    //         if(err) return cb(err);

    //         // Cast special values
    //         var values = _query.cast(result.rows[0]);

    //         results.push(values);
    //         cb();
    //       });

    //     }, function(err) {
    //       if(err) return cb(err);
    //       cb(null, results);
    //     });

    //   }, cb);
    // },

    // // Select Query Logic
    // find: function(connectionName, table, options, cb) {
    //   spawnConnection(connectionName, function __FIND__(client, cb) {

    //     // Check if this is an aggregate query and that there is something to return
    //     if(options.groupBy || options.sum || options.average || options.min || options.max) {
    //       if(!options.sum && !options.average && !options.min && !options.max) {
    //         return cb(Errors.InvalidGroupBy);
    //       }
    //     }

    //     var connectionObject = connections[connectionName];
    //     var collection = connectionObject.collections[table];

    //     var schemaName = collection.meta && collection.meta.schemaName;

    //     // Add schemaName information
    //     if (schemaName) {
    //       options._schemaName = schemaName;
    //     }

    //     // Build a Query Object
    //     var _query = new Query(collection.definition);

    //     // Grab Connection Schema
    //     var schema = {};

    //     Object.keys(connectionObject.collections).forEach(function(coll) {
    //       schema[coll] = connectionObject.collections[coll].schema;
    //     });

    //     // Build Query
    //     var _schema = collection.schema;
    //     var queryObj = new Query(_schema, schema);
    //     var query = queryObj.find(table, options);

    //     // Run Query
    //     client.query(query.query, query.values, function __FIND__(err, result) {
    //       if(err) return cb(err);

    //       // Cast special values
    //       var values = [];

    //       result.rows.forEach(function(row) {
    //         values.push(queryObj.cast(row));
    //       });

    //       // If a join was used the values should be grouped to normalize the
    //       // result into objects
    //       var _values = options.joins ? utils.group(values) : values;

    //       cb(null, _values);
    //     });

    //   }, cb);
    // },

    // // Stream one or more models from the collection
    // stream: function(collectionName, table, options, stream) {

    //   var connectionObject = connections[connectionName];
    //   var collection = connectionObject.collections[table];

    //   var client = new pg.Client(collection.config);
    //   client.connect();

    //   // Escape Table Name
    //   table = utils.escapeName(table);

    //   // Build Query
    //   var query = new Query(collection.schema).find(table, options);

    //   // Run Query
    //   var dbStream = client.query(query.query, query.values);

    //   //can stream row results back 1 at a time
    //   dbStream.on('row', function(row) {
    //     stream.write(row);
    //   });

    //   dbStream.on('error', function(err) {
    //     stream.end(); // End stream
    //     client.end(); // Close Connection
    //   });

    //   //fired after last row is emitted
    //   dbStream.on('end', function() {
    //     stream.end(); // End stream
    //     client.end(); // Close Connection
    //   });

    // },

    // // Update one or more models in the collection
    // update: function(connectionName, table, options, data, cb) {
    //   spawnConnection(connectionName, function __UPDATE__(client, cb) {

    //     var connectionObject = connections[connectionName];
    //     var collection = connectionObject.collections[table];

    //     var schemaName = collection.meta && collection.meta.schemaName;

    //     // Add schemaName information
    //     if (schemaName) {
    //       options._schemaName = schemaName;
    //     }

    //     // Build a Query Object
    //     var _query = new Query(collection.definition);

    //     // Build Query
    //     var query = new Query(collection.schema).update(table, options, data);

    //     // Run Query
    //     client.query(query.query, query.values, function __UPDATE__(err, result) {
    //       if(err) return cb(err);

    //       // Cast special values
    //       var values = [];

    //       result.rows.forEach(function(row) {
    //         values.push(_query.cast(row));
    //       });

    //       cb(null, values);
    //     });

    //   }, cb);
    // },

    // // Delete one or more models from the collection
    // destroy: function(connectionName, table, options, cb) {
    //   spawnConnection(connectionName, function __DELETE__(client, cb) {

    //     var connectionObject = connections[connectionName];
    //     var collection = connectionObject.collections[table];

    //     var schemaName = collection.meta && collection.meta.schemaName;

    //     // Add schemaName information
    //     if (schemaName) {
    //       options._schemaName = schemaName;
    //     }

    //     // Build Query
    //     var query = new Query(collection.schema).destroy(table, options);

    //     // Run Query
    //     client.query(query.query, query.values, function __DELETE__(err, result) {
    //       if(err) return cb(err);
    //       cb(null, result.rows);
    //     });

    //   }, cb);
    // }

  };

  /*************************************************************************/
  /* Private Methods
  /*************************************************************************/

  // Wrap a function in the logic necessary to provision a connection
  // (grab from the pool or create a client)
  function spawnConnection(connectionName, logic, cb) {

    var connectionObject = connections[connectionName];
    if(!connectionObject) return cb(Errors.InvalidConnection);

    // Grab a client instance from the client pool
    pg.connect(connectionObject.config, function(err, client, done) {
      after(err, client, done);
    });

    // Run logic using connection, then release/close it
    function after(err, client, done) {
      if(err) {
        console.error("Error creating a connection to DB2: " + err);

        // be sure to release connection
        done();

        return cb(err);
      }

      logic(client, function(err, result) {

        // release client connection
        done();
        return cb(err, result);
      });
    }
  }

  return adapter;
})();
