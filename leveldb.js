'use strict';

var level = require('level');
var cc = require('ceci-core');


var withdb = function(path, f) {
  return cc.go(function*() {
    var db = yield cc.nbind(level)(path, { valueEncoding: 'json' });
    try {
      return yield f(db);
    } finally {
      yield cc.nbind(db.close, db)();
    }
  });
};


var readDB = function(path, key) {
  return withdb(path, function(db) {
    var result = cc.defer();

    db.get(key, function(err, val) {
      if (!err)
        result.resolve(val);
      else if (err.notFound)
        result.resolve({});
      else
        result.reject(err);
    });

    return result;
  });
};


var writeDB = function(path, key, val) {
  return withdb(path, function(db) { return cc.nbind(db.put, db)(key, val); });
};


module.exports = function(path) {
  return cc.go(function*() {
    var cache = {
      headers   : yield readDB(path, 'headers'),
      deviations: yield readDB(path, 'deviations')
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
  
    return {
      readDependencyGraph: function() {
        return readDB(path, 'predecessors');
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
        return readDB(path, "details-" + key);
      },
      writeSingleNodeDetails: function(key, data) {
        return writeDB(path, "details-" + key, data);
      }
    };
  });
};
