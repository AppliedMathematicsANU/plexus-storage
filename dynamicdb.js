'use strict';

var cc = require('ceci-core');


module.exports = function(storage) {
  return cc.go(function*() {
    var exists = function(key) {
      return cc.go(function*() {
        return undefined !== (yield storage.read('byKey/' + key));
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
