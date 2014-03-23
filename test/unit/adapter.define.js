var adapter = require('../../lib/adapter'),
    should = require('should'),
    support = require('./support/bootstrap');

describe('adapter', function() {

  /**
   * Setup and Teardown
   */

  before(function(done) {
    support.registerConnection(['test_define', 'user'], done);
  });

  after(function(done) {
    support.Teardown('test_define', done);
  });

  // Attributes for the test table
  var definition = {
    id    : {
      type: 'serial',
      autoIncrement: true
    },
    name  : 'string',
    email : 'string',
    title : 'string',
    phone : 'string',
    type  : 'string',
    favoriteFruit : {
      defaultsTo: 'blueberry',
      type: 'string'
    },
    age   : 'integer'
  };

  /**
   * DEFINE
   *
   * Create a new table with a defined set of attributes
   */

  describe('.define()', function() {

    describe('basic usage', function() {

      // Build Table from attributes
      it('should build the table', function(done) {

        adapter.define('test', 'test_define', definition, function(err) {
          adapter.describe('test', 'test_define', function(err, result) {
            Object.keys(result).length.should.eql(8);
            done();
          });
        });

      });

    });

    describe('reserved words', function() {

      after(function(done) {
        support.Client(function(err, client, close) {
          var query = 'DROP TABLE "user";';
          client.query(query, function(err) {

            // close client
            close();

            done();
          });
        });
      });

      // Build Table from attributes
      it('should escape reserved words', function(done) {

        adapter.define('test', 'user', definition, function(err) {
          adapter.describe('test', 'user', function(err, result) {
            Object.keys(result).length.should.eql(8);
            done();
          });
        });

      });

    });

  });
});
