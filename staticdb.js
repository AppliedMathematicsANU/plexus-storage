'use strict';

var cc = require('ceci-core');


module.exports = function(storage) {
  return cc.go(function*() {
    var cache = {
      headers   : yield storage.read('headers'),
      deviations: yield storage.read('deviations')
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

      return storage.write(table, val);
    };
  
    var detailsKey = function(key) {
      return 'details/' + key.slice(0, 6) + '/' + key;
    };

    return {
      readDependencyGraph: function() {
        return storage.read('predecessors');
      },
      writeDependencyGraph: function(val) {
        return storage.write('predecessors', val);
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
        return storage.read(detailsKey(key));
      },
      writeSingleNodeDetails: function(key, data) {
        return storage.write(detailsKey(key), data);
      },
      close: storage.close
    };
  });
};
