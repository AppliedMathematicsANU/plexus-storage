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


module.exports = function(path) {
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
        yield cc.nbind(exec)("mkdir -p '" + p.dirname(loc) + "'");
        return yield cc.nbind(fs.writeFile, fs)(
          loc, text, { encoding: 'utf8' });
      });
    },

    close: function() {
    }
  }
}
