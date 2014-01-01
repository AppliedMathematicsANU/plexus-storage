'use strict';

var fs   = require('fs');
var p    = require('path');
var exec = require('child_process').exec;

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

var readJSON = function(path) {
  var result = cc.defer();

  fs.exists(path, function(exists) {
    if (exists)
      cc.go(function*() {
        result.resolve(JSON.parse(yield nbind(fs.readFile, fs)(path)));
      });
    else
      result.resolve({});
  });

  return result;
};

var writeJSON = function(path, val) {
  var text = JSON.stringify(val, null, 4);

  return cc.go(function*() {
    yield nbind(exec)("mkdir -p '" + p.dirname(path) + "'");
    return yield nbind(fs.writeFile, fs)(path, text, { encoding: 'utf8' });
  });
};

module.exports = function(path) {
  return cc.go(function*() {
    var cache = {
      headers   : yield readJSON(path + "/headers.json"),
      deviations: yield readJSON(path + "/deviations.json")
    };

    var read = function(table, keys) {
      var out = {};
      keys.forEach(function(k) {
        out[k] = cache[table][k] || {};
      });

      var result = cc.defer();
      result.resolve(out);
      return result;
    };

    var write = function(table, data) {
      var val = cache[table];
      var k;
      for (k in data)
        val[k] = data[k];

      return cc.go(function*() {
        return yield writeJSON(path + '/' + table + '.json', val);
      });
    };
  
    var detailsPath = function(key) {
      return path + "/details/" + key.slice(0, 6) + "/" + key + ".json";
    };

    return {
      readDependencyGraph: function() {
        return readJSON(path + "/graph.json");
      },
      writeDependencyGraph: function(val) {
        return writeJSON(path + "/graph.json", val);
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
        return readJSON(detailsPath(key));
      },
      writeSingleNodeDetails: function(key, data) {
        return writeJSON(detailsPath(key), data);
      }
    };
  });
};
