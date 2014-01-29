'use strict';

var cc   = require('ceci-core');
var chan = require('ceci-channels');
var memdown = require('memdown');

var level = require('./leveldb');


var show = function(key, val) {
  console.log(key + ': ' + JSON.stringify(val, null, 2));
};


cc.go(function*() {
  var db = yield level('./dummy', { db: memdown });

  var batch = db.batch()
    .put('name', 'olaf')
    .put('age', 50)
    .put('weight', { amount: 87, unit: 'kg' })
    .write();

  yield batch;

  for (var key in { name: 0, age: 0, weight: 0 })
    show(key, yield db.read(key));
  console.log();

  yield chan.each(
    function(data) { show(data.key, data.value); },
    db.readRange({ reverse: true }));

  yield db.close();
});
