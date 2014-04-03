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

        return cc.go(wrapGenerator.mark(function() {
          return wrapGenerator(function($ctx0) {
            while (1) switch ($ctx0.next) {
            case 0:
              $ctx0.next = 2;
              return exists(loc);
            case 2:
              if (!$ctx0.sent) {
                $ctx0.next = 10;
                break;
              }

              $ctx0.next = 5;
              return cc.nbind(fs.readFile, fs)(loc);
            case 5:
              $ctx0.t0 = $ctx0.sent;
              $ctx0.rval = JSON.parse($ctx0.t0);
              delete $ctx0.thrown;
              $ctx0.next = 10;
              break;
            case 10:
            case "end":
              return $ctx0.stop();
            }
          }, this);
        }));
      },

      write: function(key, val) {
        var loc = makeLocation(path, key);
        var text = JSON.stringify(val, null, 4);

        return cc.go(wrapGenerator.mark(function() {
          return wrapGenerator(function($ctx1) {
            while (1) switch ($ctx1.next) {
            case 0:
              $ctx1.next = 2;
              return cc.nbind(exec)("mkdir -p '" + fspath.dirname(loc) + "'");
            case 2:
              $ctx1.next = 4;

              return cc.nbind(fs.writeFile, fs)(
                loc, text, { encoding: 'utf8' })
            case 4:
              $ctx1.rval = $ctx1.sent;
              delete $ctx1.thrown;
              $ctx1.next = 8;
              break;
            case 8:
            case "end":
              return $ctx1.stop();
            }
          }, this);
        }));
      },

      close: function() {
      }
    }
  },

  leveldb: function(path, options) {
    return cc.go(wrapGenerator.mark(function() {
      var db;

      return wrapGenerator(function($ctx2) {
        while (1) switch ($ctx2.next) {
        case 0:
          options = merge({ valueEncoding: 'json' }, options);
          $ctx2.next = 3;
          return cc.nbind(levelup)(path, options);
        case 3:
          db = $ctx2.sent;

          $ctx2.rval = {
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

          delete $ctx2.thrown;
          $ctx2.next = 8;
          break;
        case 8:
        case "end":
          return $ctx2.stop();
        }
      }, this);
    }));
  }
};


module.exports = function(engineType, path, options) {
  return cc.go(wrapGenerator.mark(function() {
    var db, read, write, cache, readBatch, writeBatch, detailsKey;

    return wrapGenerator(function($ctx3) {
      while (1) switch ($ctx3.next) {
      case 0:
        $ctx3.next = 2;
        return engines[engineType](path, options);
      case 2:
        db = $ctx3.sent;

        read = function(key) {
          return cc.go(wrapGenerator.mark(function() {
            var val;

            return wrapGenerator(function($ctx4) {
              while (1) switch ($ctx4.next) {
              case 0:
                $ctx4.next = 2;
                return db.read(key);
              case 2:
                val = $ctx4.sent;
                $ctx4.rval = (val === undefined) ? {} : val;
                delete $ctx4.thrown;
                $ctx4.next = 7;
                break;
              case 7:
              case "end":
                return $ctx4.stop();
              }
            }, this);
          }));
        };

        write = db.write;
        $ctx3.next = 7;
        return read('headers');
      case 7:
        $ctx3.t1 = $ctx3.sent;
        $ctx3.next = 10;
        return read('deviations');
      case 10:
        $ctx3.t2 = $ctx3.sent;

        cache = {
          headers: $ctx3.t1,
          deviations: $ctx3.t2
        };

        readBatch = function(table, keys) {
          var out = {};
          keys.forEach(function(k) { out[k] = cache[table][k] || {}; });
          return out;
        };

        writeBatch = function(table, data) {
          var val = cache[table];
          for (var k in data)
            val[k] = data[k];

          return write(table, val);
        };

        detailsKey = function(key) {
          return 'details/' + key.slice(0, 6) + '/' + key;
        };

        $ctx3.rval = {
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

        delete $ctx3.thrown;
        $ctx3.next = 19;
        break;
      case 19:
      case "end":
        return $ctx3.stop();
      }
    }, this);
  }));
};
