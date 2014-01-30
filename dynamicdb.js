'use strict';

var timestamp = require('monotonic-timestamp');

var cc   = require('ceci-core');
var chan = require('ceci-channels');

var util = require('./util');


module.exports = function(storage) {
  return cc.go(function*() {
    var path = function() {
      var args = Array.prototype.slice.call(arguments);
      return args.join('/');
    };

    var readAttributes = function(key) {
      return storage.read(path('attr', key));
    };

    var readSuccessors = function(key) {
      return storage.read(path('succ', key));
    };

    var writeAttributes = function(key, attr) {
      return cc.go(function*() {
        var t = timestamp().toString(36);

        return yield storage.batch()
          .put(path('keys', key), t)
          .put(path('attr', key), attr)
          .put(path('hist', 'attr', key, t), attr)
          .write();
      });
    };

    var writeSuccessors = function(key, succ) {
      return cc.go(function*() {
        var t = timestamp().toString(36);

        return yield storage.batch()
          .put(path('keys', key), t)
          .put(path('succ', key), succ)
          .put(path('hist', 'succ', key, t), succ)
          .write();
      });
    };

    var destroy = function(key) {
      return cc.go(function*() {
        var t = timestamp().toString(36);

        return yield storage.batch()
          .del(path('keys', key))
          .del(path('attr', key))
          .del(path('succ', key))
          .put(path('hist', 'attr', key, t), null)
          .put(path('hist', 'succ', key, t), null)
          .write();
      });
    };

    return {
      readAttributes: function(key) {
        return readAttributes(key);
      },
      readSuccessors: function(key) {
        return readSuccessors(key);
      },
      writeAttributes: function(key, attr) {
        return writeAttributes(key, attr);
      },
      writeSuccessors: function(key, attr) {
        return writeSuccessors(key, attr);
      },
      destroy: function(key) {
        return destroy(key);
      },
      close: storage.close
    };
  })
};
