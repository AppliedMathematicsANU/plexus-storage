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


var formatAttributes = function(db, key) {
  return cc.go(function*() {
    return key + ': ' + JSON.stringify(yield db.readAttributes(key), null, 2);
  });
};


cc.go(function*() {
  var db  = yield level('', { db: memdown });
  var dyn = yield dynamic(db);

  yield dyn.writeAttributes('olaf', {
    age: 50,
    weight: { amount: 87.5, unit: 'kg' },
    height: { amount: 187, unit: 'cm' }
  });

  yield dyn.writeAttributes('delaney', {
    age: 5,
    weight: { amount: 2.5, unit: 'kg' },
    height: { amount: 25, unit: 'mm' }
  });

  console.log(yield formatAttributes(dyn, 'olaf'));
  console.log(yield formatAttributes(dyn, 'delaney'));
  console.log();

  console.log('--- full database contents: ---');
  yield dump_db(db);
  console.log();

  console.log('--- after deleting delaney: ---');
  yield dyn.destroy('delaney');
  yield dump_db(db);
  console.log();

  dyn.close();
});
