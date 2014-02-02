'use strict';

var cc   = require('ceci-core');
var chan = require('ceci-channels');
var memdown = require('memdown');

var level = require('./leveldb');
var curated = require('./curated');


var show = function(key, val) {
  console.log(key + ': ' + JSON.stringify(val, null, 2));
};

var dump_db = function(db, options) {
  return chan.each(
    function(data) { show(data.key, data.value); },
    db.readRange(options));
};


var formatData = function(db, key) {
  return cc.go(function*() {
    var tmp = {};
    tmp[key] = {
      attr: yield db.readAttributes(key),
      succ: yield db.readSuccessors(key)
    };
    return JSON.stringify(tmp, null, 2);
  });
};


cc.go(function*() {
  var db  = yield level('', { db: memdown });
  var dyn = yield curated(db);

  yield dyn.writeAttributes('olaf', {
    age: 50,
    weight: { amount: 87.5, unit: 'kg' },
    height: { amount: 187, unit: 'cm' }
  });

  yield dyn.writeSuccessors('olaf', ['delaney', 'ada', 'grace']);

  yield dyn.writeAttributes('delaney', {
    age: 5,
    weight: { amount: 2.5, unit: 'kg' },
    height: { amount: 25, unit: 'mm' }
  });

  yield dyn.writeSuccessors('delaney', ['mathew', 'samuel']);

  console.log(yield formatData(dyn, 'olaf'));
  console.log(yield formatData(dyn, 'delaney'));
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
