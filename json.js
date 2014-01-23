'use strict';

var fs   = require('fs');
var p    = require('path');
var exec = require('child_process').exec;

var cc = require('ceci-core');


var exists = function(path) {
  var result = cc.defer();
  fs.exists(path, function(exists) { result.resolve(exists); });
  return result;
};


var makeLocation = function(path, key) {
  return path + '/' + key + '.json';
};


var readDB = function(path, key) {
  var loc = makeLocation(path, key);

  return cc.go(function*() {
    if (yield exists(loc))
      return JSON.parse(yield cc.nbind(fs.readFile, fs)(loc));
    else
      return {};
  });
};


var writeDB = function(path, key, val) {
  var loc = makeLocation(path, key);
  var text = JSON.stringify(val, null, 4);

  return cc.go(function*() {
    yield cc.nbind(exec)("mkdir -p '" + p.dirname(loc) + "'");
    return yield cc.nbind(fs.writeFile, fs)(loc, text, { encoding: 'utf8' });
  });
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
  
    var detailsKey = function(key) {
      return 'details/' + key.slice(0, 6) + '/' + key;
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
        return readDB(path, detailsKey(key));
      },
      writeSingleNodeDetails: function(key, data) {
        return writeDB(path, detailsKey(key), data);
      }
    };
  });
};
