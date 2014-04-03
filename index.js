'use strict';

var fs      = require('fs');
var fspath  = require('path');
var exec    = require('child_process').exec;

var levelup = require('levelup');
var cc      = require('ceci-core');


var merge = function(obj1, obj2) {
  var result = {};
  var key;
  for (key in obj1)
    result[key] = obj1[key];
  for (key in obj2)
    result[key] = obj2[key];
  return result;
};


var exists = function(path) {
  var result = cc.defer();
  fs.exists(path, function(exists) { result.resolve(exists); });
  return result;
};

var makeLocation = function(path, key) {
  return path + '/' + key + '.json';
};


var engines = {
  json: function(path) {
    return {
      read: function(key) {
        var loc = makeLocation(path, key);

        return cc.go(function*() {
          if (yield exists(loc))
            return JSON.parse(yield cc.nbind(fs.readFile, fs)(loc));
        });
      },

      write: function(key, val) {
        var loc = makeLocation(path, key);
        var text = JSON.stringify(val, null, 4);

        return cc.go(function*() {
          yield cc.nbind(exec)("mkdir -p '" + fspath.dirname(loc) + "'");
          return yield cc.nbind(fs.writeFile, fs)(
            loc, text, { encoding: 'utf8' });
        });
      },

      close: function() {
      }
    }
  },

  leveldb: function(path, options) {
    return cc.go(function*() {
      options = merge({ valueEncoding: 'json' }, options)
      var db = yield cc.nbind(levelup)(path, options);

      return {
        read: function(key) {
          var result = cc.defer();

          db.get(key, function(err, val) {
            if (!err)
              result.resolve(val);
            else if (err.notFound)
              result.resolve();
            else
              result.reject(err);
          });

          return result;
        },

        write: cc.nbind(db.put, db),
        close: cc.nbind(db.close, db)
      };
    });
  }
};


module.exports = function(engineType, path, options) {
  return cc.go(function*() {
    var db = yield engines[engineType](path, options);

    var read = function(key) {
      return cc.go(function*() {
        var val = yield db.read(key);
        return (val === undefined) ? {} : val;
      });
    };

    var write = db.write;

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
      close: db.close
    };
  });
};
