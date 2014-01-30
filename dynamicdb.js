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

    var writeAttributes = function(key, attr) {
      return cc.go(function*() {
        var t = timestamp().toString(36);

        return yield storage.batch()
          .put(path('keys', key), t)
          .put(path('attr', key), attr || {})
          .put(path('hist', 'attr', key, t), attr || {})
          .write();
      });
    };

    return {
      readAttributes: function(key) {
        return readAttributes(key);
      },
      writeAttributes: function(key, attr) {
        return writeAttributes(key, attr);
      },
      destroy: function(key) {
        return destroy(key);
      },
      close: storage.close
    };
  })
};
