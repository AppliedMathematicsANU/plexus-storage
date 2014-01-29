'use strict';

exports.merge = function(obj1, obj2) {
  var result = {};
  var key;
  for (key in obj1)
    result[key] = obj1[key];
  for (key in obj2)
    result[key] = obj2[key];
  return result;
};
