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

    var exists = function(key) {
      return cc.go(function*() {
        return undefined !== (yield storage.read(path('attr', key)));
      });
    };

    var createOrUpdate = function(key, spec) {
      return cc.go(function*() {
        var attr = (yield storage.read(path('attr', key))) || {}
        if (spec)
          attr = util.merge(attr, spec)

        var t = timestamp().toString(36);

        return yield storage.batch()
          .put(path('attr', key), attr)
          .put(path('history', key, t), attr)
          .write();
      });
    };

    return {
      create: function(key, spec) {
        return cc.go(function*() {
          if (yield exists(key))
            throw new Error('\'' + key + '\' already exists in database');
          else
            return yield createOrUpdate(key, spec);
        });
      },
      read: function(key) {
        return cc.go(function*() {
          if (yield exists(key))
            return yield retrieve(key);
          else
            throw new Error('\'' + key + '\' does not exists in database');
        });
      },
      update: function(key, spec) {
        return cc.go(function*() {
          if (yield exists(key))
            return yield createOrUpdate(key, spec);
          else
            throw new Error('\'' + key + '\' does not exists in database');
        });
      },
      destroy: function() {
        return cc.go(function*() {
          if (yield exists(key))
            return yield obliterate(key);
          else
            throw new Error('\'' + key + '\' does not exists in database');
        });
      },
      close: storage.close
    };
  })
};
