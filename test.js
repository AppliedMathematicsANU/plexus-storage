'use strict';

var cc   = require('ceci-core');
var chan = require('ceci-channels');

var level = require('./leveldb');

cc.go(function*() {
  var db = yield level('./testdb');

  var batch = db.batch()
    .put('name', 'olaf')
    .put('age', 50)
    .put('weight', 87)
    .put('weight-unit', 'kg')
    .write();

  yield batch;

  for (var key in { name: 0, age: 0, weight: 0, 'weight-unit': 0 })
    console.log(key + ': ' + (yield db.read(key)));
  console.log();

  yield chan.each(
    function(data) { console.log(data.key + ': ' + data.value); },
    db.readRange({ reverse: true }));

  yield db.close();
});
