'use strict';

var level = require('level');
var cc = require('ceci-core');


var readDB = function(db, key) {
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
};


var writeDB = function(db, key, val) {
  return cc.nbind(db.put, db)(key, val);
};


module.exports = function(path) {
  var withdb = function(f) {
    return cc.go(function*() {
      var db = yield cc.nbind(level)(path, { valueEncoding: 'json' });
      try {
        return yield f(db);
      } finally {
        yield cc.nbind(db.close, db)();
      }
    });
  };

  return cc.go(function*() {
    var cache = {
      headers:
      yield withdb(function(db) { return readDB(db, 'headers'); }),

      deviations:
      yield withdb(function(db) { return readDB(db, 'deviations'); }),
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

      return withdb(function(db) {
        return writeDB(db, table, val);
      });
    };
  
    return {
      readDependencyGraph: function() {
        return withdb(function(db) {
          return readDB(db, 'predecessors');
        });
      },
      writeDependencyGraph: function(val) {
        return withdb(function(db) {
          return writeDB(db, 'predecessors', val);
        });
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
        return withdb(function(db) {
          return readDB(db, "details-" + key);
        });
      },
      writeSingleNodeDetails: function(key, data) {
        return withdb(function(db) {
          return writeDB(db, "details-" + key, data);
        });
      }
    };
  });
};
