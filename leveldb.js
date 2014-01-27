'use strict';

var level = require('level');
var cc = require('ceci-core');


var withdb = function(path, f) {
  return cc.go(function*() {
    var db = yield cc.nbind(level)(path, { valueEncoding: 'json' });
    try {
      return yield f(db);
    } finally {
      yield cc.nbind(db.close, db)();
    }
  });
};


exports.readDB = function(path, key) {
  return withdb(path, function(db) {
    var result = cc.defer();

    db.get(key, function(err, val) {
      if (!err)
        result.resolve(val);
      else if (err.notFound)
        result.resolve({});
      else
        result.reject(err);
    });

    return result;
  });
};


exports.writeDB = function(path, key, val) {
  return withdb(path, function(db) {
    return cc.nbind(db.put, db)(key, val);
  });
};
