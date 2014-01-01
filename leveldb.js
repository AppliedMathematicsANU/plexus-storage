'use strict';

var level = require('level');
var cc = require('ceci-core');


var nbind = function(fn, context) {
  return function() {
    var args = Array.prototype.slice.call(arguments);
    var result = cc.defer();

    fn.apply(context, args.concat(function(err, val) {
      if (err)
        result.reject(new Error(err));
      else
        result.resolve(val);
    }));

    return result;
  };
};


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
  return nbind(db.put, db)(key, val);
};


var mapHash = function(h, f) {
  return Object.keys(h).map(function(key) {
    return f(key, h[key]);
  });
};

var zipmap = function(keys, values) {
  var n = Math.min(keys.length, values.length);
  var result = {};

  for (var i = 0; i < n; ++i)
    result[keys[i]] = values[i];

  return result;
};


module.exports = function(path) {
  var withdb = function(f) {
    return cc.go(function*() {
      var db = nbind(level)(path, { valueEncoding: 'json' });
      try {
        return yield f(db);
      } finally {
        yield nbind(db.close, db);
      }
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
      return withdb(function(db) {
        return cc.go(function*() {
          var result = {};
          for (var i = 0; i < keys.length; ++i)
            result[keys[i]] = yield readDB(db, "headers-" + keys[i]);
          return result;
        });
      });
    },
    writeSomeHeaders: function(data) {
      return withdb(function(db) {
        cc.go(function*() {
          for (var key in data)
            yield writeDB(db, "headers-" + key, data[key]);
        });
      });
    },
    readSomeDeviations: function(keys) {
      return withdb(function(db) {
        return cc.go(function*() {
          var result = {};
          for (var i = 0; i < keys.length; ++i)
            result[keys[i]] = yield readDB(db, "deviations-" + keys[i]);
          return result;
        });
      });
    },
    writeSomeDeviations: function(data) {
      return withdb(function(db) {
        cc.go(function*() {
          for (var key in data)
            yield writeDB(db, "deviations-" + key, data[key]);
        });
      });
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
};
