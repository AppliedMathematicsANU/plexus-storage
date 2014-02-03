'use strict';

var memdown = require('memdown');
var level   = require('./leveldb');

var cc   = require('ceci-core');
var chan = require('ceci-channels');
var cf   = require('ceci-filters');

var curated = require('./curated');


var show = function(key, val) {
  console.log(JSON.stringify(key) + ': ' + JSON.stringify(val, null, 2));
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


var asArray = function(ch) {
  return cc.go(function*() {
    var res = [];
    var val;
    while (undefined !== (val = yield chan.pull(ch)))
      res.push(val);
    return res;
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

  yield dyn.addRelation('olaf', 'delaney');
  yield dyn.addRelation('olaf', 'ada');
  yield dyn.addRelation('olaf', 'grace');

  yield dyn.writeAttributes('delaney', {
    age: 5,
    weight: { amount: 2.5, unit: 'kg' },
    height: { amount: 25, unit: 'mm' }
  });

  yield dyn.addRelation('delaney', 'mathew');
  yield dyn.addRelation('delaney', 'samuel');

  console.log(yield formatData(dyn, 'olaf'));
  console.log(yield formatData(dyn, 'delaney'));
  console.log();

  console.log('successors of olaf: ' +
              (yield asArray(dyn.readSuccessors('olaf'))));
  console.log();

  console.log('predecessors of delaney:' +
              (yield asArray(dyn.readPredecessors('delaney'))));
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
