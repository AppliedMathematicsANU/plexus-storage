'use strict';

var level = require('level');
var cc = require('ceci-core');


module.exports = function(path) {
  return cc.go(function*() {
    var db = yield cc.nbind(level)(path, { valueEncoding: 'json' });

    try {
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
      }
    } catch(ex) {
      yield cc.nbind(db.close, db)();
      throw new Error(ex);
    }
  });
};
