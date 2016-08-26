Sails-DB2 Adapter
===

[**IBM DB2**](http://www-01.ibm.com/software/data/db2/) adapter for the Sails framework and Waterline ORM. Allows you to use DB2 via your models to store and retrieve data. Also provides a query() method for a direct interface to execute raw SQL commands.

## Installation

Sails-DB2 uses [**ibm_db**](https://www.npmjs.org/package/ibm_db) driver to interact with the db, so install it first.

Then, install this adapter via [**NPM**](https://www.npmjs.org/):

```bash
$ npm install sails-db2
```

## Sails Configuration

Add the db2 config to the config/adapters.js file. Basic options:

```javascript
module.exports.adapters = {
  default: 'db2',

  db2: {
    module   : 'sails-db2',
    host     : 'localhost',
    port     : 50000,
    user     : 'username',
    password : 'password',
    database : 'DB2 Database Name',
    schemaDB2: 'my_schema'
  }
};
```