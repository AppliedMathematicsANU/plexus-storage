'use strict';

var memdown  = require('memdown');

var cc   = require('ceci-core');
var chan = require('ceci-channels');

var level   = require('./leveldb');
var curated = require('./curated');
var util    = require('./util');


var show = function(key, val) {
  console.log(JSON.stringify(key) + ': ' + JSON.stringify(val, null, 2));
};

var dump_db = function(db, options) {
  return chan.each(
    function(data) { show(util.decode(data.key), data.value); },
    db.readRange(options));
};


var formatData = function(db, key) {
  return cc.go(function*() {
    var tmp = {};
    tmp[key] = (yield db.readAttributes(key)) || null;
    return JSON.stringify(tmp, null, 2);
  });
};


var asArray = function(ch) {
  return cc.go(function*() {
    var res = [];
    yield chan.each(function(x) { res.push(x); }, ch);
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

  console.log('predecessors of delaney: ' +
              (yield asArray(dyn.readPredecessors('delaney'))));
  console.log();

  console.log('--- full database contents: ---');
  yield dump_db(db);
  console.log();

  console.log('--- after deleting delaney: ---');
  yield dyn.destroy('delaney');

  console.log(yield formatData(dyn, 'olaf'));
  console.log(yield formatData(dyn, 'delaney'));
  console.log();

  console.log('successors of olaf: ' +
              (yield asArray(dyn.readSuccessors('olaf'))));
  console.log();

  console.log('predecessors of delaney: ' +
              (yield asArray(dyn.readPredecessors('delaney'))));
  console.log();

  yield dump_db(db);
  console.log();

  dyn.close();
});
