'use strict';

var cc   = require('ceci-core');
var chan = require('ceci-channels');
var memdown = require('memdown');

var level = require('./leveldb');
var dynamic = require('./dynamicdb');


var show = function(key, val) {
  console.log(key + ': ' + JSON.stringify(val, null, 2));
};

var dump_db = function(db, options) {
  return chan.each(
    function(data) { show(data.key, data.value); },
    db.readRange(options));
};


cc.go(function*() {
  var db  = yield level('', { db: memdown });
  var dyn = yield dynamic(db);

  yield dyn.create('olaf', {
    age: 50,
    weight: { amount: 87, unit: 'kg' } });

  yield dyn.update('olaf', {
    weight: { amount: 87.5, unit: 'kg' },
    height: { amount: 187, unit: 'cm' }
  });

  yield dump_db(db);

  dyn.close();
});
