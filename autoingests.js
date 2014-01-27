'use strict';

var cc = require('ceci-core');


module.exports = function(path, dblib) {
  return cc.go(function*() {
    var db = yield dblib(path);

    var cache = {
      headers   : yield db.read('headers'),
      deviations: yield db.read('deviations')
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

      return db.write(table, val);
    };
  
    var detailsKey = function(key) {
      return 'details/' + key.slice(0, 6) + '/' + key;
    };

    return {
      readDependencyGraph: function() {
        return db.read('predecessors');
      },
      writeDependencyGraph: function(val) {
        return db.write('predecessors', val);
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
        return db.read(detailsKey(key));
      },
      writeSingleNodeDetails: function(key, data) {
        return db.write(detailsKey(key), data);
      }
    };
  });
};
