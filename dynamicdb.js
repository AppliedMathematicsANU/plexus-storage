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

    var write = function(key, spec) {
      return cc.go(function*() {
        var t = timestamp().toString(36);

        return yield storage.batch()
          .put(path('attr', key), spec)
          .put(path('history', key, t), spec)
          .write();
      });
    };

    return {
      read: function(key) {
        return read(key);
      },
      write: function(key, spec) {
        return write(key, spec);
      },
      destroy: function() {
        return destroy(key);
      },
      close: storage.close
    };
  })
};
