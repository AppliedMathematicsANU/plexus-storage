'use strict';

var bops     = require('bops');
var bytewise = require('bytewise');


exports.encode = function(data) {
  return bops.to(bytewise.encode(data), 'base64');
};


exports.decode = function(code) {
  return bytewise.decode(bops.from(code, 'base64'));
};


exports.merge = function(obj1, obj2) {
  var result = {};
  var key;
  for (key in obj1)
    result[key] = obj1[key];
  for (key in obj2)
    result[key] = obj2[key];
  return result;
};