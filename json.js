// Generated by CoffeeScript 1.6.3
var Q, exec, fs, lazy, p, readJSON, writeJSON;

Q = require('q');

fs = require('fs');

p = require('path');

exec = require('child_process').exec;

lazy = function(f) {
  var val;
  val = void 0;
  return function() {
    return val != null ? val : val = f();
  };
};

readJSON = function(path) {
  var deferred;
  deferred = Q.defer();
  fs.exists(path, function(exists) {
    return deferred.resolve(exists ? Q.ninvoke(fs, 'readFile', path, {
      encoding: 'utf8'
    }).then(JSON.parse) : Q({}));
  });
  return deferred.promise;
};

writeJSON = function(path, val) {
  return Q.nfcall(exec, "mkdir -p '" + (p.dirname(path)) + "'").then(function() {
    var text;
    text = JSON.stringify(val, null, 4);
    return Q.ninvoke(fs, 'writeFile', path, text, {
      encoding: 'utf8'
    });
  });
};

module.exports = function(path, cb) {
  var cache, db, detailsPath, read, write;
  cache = {
    headers: lazy(function() {
      return readJSON("" + path + "/headers.json");
    }),
    deviations: lazy(function() {
      return readJSON("" + path + "/deviations.json");
    })
  };
  read = function(table, keys) {
    return cache[table]().then(function(val) {
      var k, out, _i, _len;
      out = {};
      for (_i = 0, _len = keys.length; _i < _len; _i++) {
        k = keys[_i];
        out[k] = val[k] || {};
      }
      return out;
    });
  };
  write = function(table, data) {
    return cache[table]().then(function(val) {
      var k, v;
      for (k in data) {
        v = data[k];
        val[k] = v;
      }
      return writeJSON("" + path + "/" + table + ".json", val);
    });
  };
  detailsPath = function(key) {
    return "" + path + "/details/" + key.slice(0, 6) + "/" + key + ".json";
  };
  db = {
    readDependencyGraph: function(cb) {
      return readJSON("" + path + "/graph.json").nodeify(cb);
    },
    writeDependencyGraph: function(val, cb) {
      return writeJSON("" + path + "/graph.json", val).nodeify(cb);
    },
    readSomeHeaders: function(keys, cb) {
      return read('headers', keys).nodeify(cb);
    },
    writeSomeHeaders: function(data, cb) {
      return write('headers', data).nodeify(cb);
    },
    readSomeDeviations: function(keys, cb) {
      return read('deviations', keys).nodeify(cb);
    },
    writeSomeDeviations: function(data, cb) {
      return write('deviations', data).nodeify(cb);
    },
    readSingleNodeDetails: function(key, cb) {
      return readJSON(detailsPath(key)).nodeify(cb);
    },
    writeSingleNodeDetails: function(key, data, cb) {
      return writeJSON(detailsPath(key), data).nodeify(cb);
    }
  };
  return cb(null, db);
};
