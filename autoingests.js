'use strict';

var cc = require('ceci-core');


module.exports = function(path, dblib) {
  return cc.go(function*() {
    var cache = {
      headers   : yield dblib.readDB(path, 'headers'),
      deviations: yield dblib.readDB(path, 'deviations')
    };

    var read = function(table, keys) {
      var out = {};
      keys.forEach(function(k) { out[k] = cache[table][k] || {}; });
      return out;
    };

    var write = function(table, data) {
      var val = cache[table];
      for (var k in data)
        val[k] = data[k];

      return writeDB(path, table, val);
    };
  
    var detailsKey = function(key) {
      return 'details/' + key.slice(0, 6) + '/' + key;
    };

    return {
      readDependencyGraph: function() {
        return dblib.readDB(path, 'predecessors');
      },
      writeDependencyGraph: function(val) {
        return writeDB(path, 'predecessors', val);
      },
      readSomeHeaders: function(keys) {
        return read('headers', keys);
      },
      writeSomeHeaders: function(data) {
        return write('headers', data);
      },
      readSomeDeviations: function(keys) {
        return read('deviations', keys);
      },
      writeSomeDeviations: function(data) {
        return write('deviations', data);
      },
      readSingleNodeDetails: function(key) {
        return dblib.readDB(path, detailsKey(key));
      },
      writeSingleNodeDetails: function(key, data) {
        return writeDB(path, detailsKey(key), data);
      }
    };
  });
};
