'use strict';

var cc = require('ceci-core');


module.exports = function(storage) {
  return cc.go(function*() {
    var read = function(key) {
      return cc.go(function*() {
        var val = yield storage.read(key);
        return (val === undefined) ? {} : val;
      });
    };

    var write = storage.write;

    var cache = {
      headers   : yield read('headers'),
      deviations: yield read('deviations')
    };

    var readBatch = function(table, keys) {
      var out = {};
      keys.forEach(function(k) { out[k] = cache[table][k] || {}; });
      return out;
    };

    var writeBatch = function(table, data) {
      var val = cache[table];
      for (var k in data)
        val[k] = data[k];

      return write(table, val);
    };
  
    var detailsKey = function(key) {
      return 'details/' + key.slice(0, 6) + '/' + key;
    };

    return {
      readDependencyGraph: function() {
        return read('predecessors');
      },
      writeDependencyGraph: function(val) {
        return write('predecessors', val);
      },
      readSomeHeaders: function(keys) {
        return readBatch('headers', keys);
      },
      writeSomeHeaders: function(data) {
        return writeBatch('headers', data);
      },
      readSomeDeviations: function(keys) {
        return readBatch('deviations', keys);
      },
      writeSomeDeviations: function(data) {
        return writeBatch('deviations', data);
      },
      readSingleNodeDetails: function(key) {
        return read(detailsKey(key));
      },
      writeSingleNodeDetails: function(key, data) {
        return write(detailsKey(key), data);
      },
      close: storage.close
    };
  });
};
