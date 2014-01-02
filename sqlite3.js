'use strict';

var fs  = require('fs');
var any = require('any-db');

var cc = require('ceci-core');


var runQuery = function(db) {
  return cc.nbind(db.query, db)(Array.prototype.slice.call(arguments));
};


var createTable = function(db, name) {
  var query = "CREATE TABLE " + name + " (id text PRIMARY KEY, content text)";
  return runQuery(query);
};

var readTable = function(db, name, keys) {
  return cc.go(function*() {
    var keylist, query, result;

    if (keys != null) {
      keylist = keys.map(function(s) { return "'" + s + "'"; }).join(", ");
      query = "SELECT * FROM " + name + " WHERE id IN (" + keylist + ")";
    } else
      query = "SELECT * FROM " + name;

    result = {};

    (yield runQuery(query)).rows.forEach(function(row) {
      result[row.id] = JSON.parse(row.content);
    });

    if (keys != null)
      keys.forEach(function(k) {
        if (result[k] == null)
          result[k] = {};
      });

    return result;
  });
};

var writeTable = function(db, name, data) {
  return cc.go(function*() {
    for (var key in data) {
      yield runQuery(db,
                     "INSERT OR REPLACE INTO " + name + " VALUES (?, ?)",
                     [key, JSON.stringify(data[key])]);
    }
  });
};

module.exports = function(path, cb) {
  var withdb = function(f) {
    return cc.go(function*() {
      var db = yield cc.nbind(any.createConnection)("sqlite3://" + path);
      try {
        return yield f(db);
      } finally {
        yield cc.nbind(db.end, db);
      }
    });
  };

  var storage = {
    readDependencyGraph: function() {
      return withdb(function(db) {
        return readTable(db, 'predecessors');
      });
    },
    writeDependencyGraph: function(val) {
      return withdb(function(db) {
        return writeTable(db, 'predecessors', val);
      });
    },
    readSomeHeaders: function(keys) {
      return withdb(function(db) {
        return readTable(db, 'headers', keys);
      });
    },
    writeSomeHeaders: function(data) {
      return withdb(function(db) {
        return writeTable(db, 'headers', data);
      });
    },
    readSomeDeviations: function(keys) {
      return withdb(function(db) {
        return readTable(db, 'deviations', keys);
      });
    },
    writeSomeDeviations: function(data) {
      return withdb(function(db) {
        return writeTable(db, 'deviations', data);
      });
    },
    readSingleNodeDetails: function(key) {
      return withdb(function(db) {
        return cc.go(function*() {
          var data = yield readTable(db, 'details', [key]);
          return data[key];
        });
      });
    },
    writeSingleNodeDetails: function(key, val) {
      var data = {};
      data[key] = val;
      return withdb(function(db) {
        return writeTable(db, 'details', data);
      });
    }
  };

  var result = cc.defer();

  fs.exists(path, function(exists) {
    withdb(function(db) {
      cc.go(function*() {
        if (!exists) {
          yield createTable(db, 'predecessors');
          yield createTable(db, 'headers');
          yield createTable(db, 'details');
          yield createTable(db, 'deviations');
        }
        result.result(storage);
      });
    });
  });

  return result;
};
