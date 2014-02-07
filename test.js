'use strict';

var memdown  = require('memdown');

var cc   = require('ceci-core');
var chan = require('ceci-channels');

var level   = require('./leveldb');
var curated = require('./curated');
var util    = require('./util');


var showPair = function(key, val) {
  console.log(JSON.stringify(key) + ': ' + JSON.stringify(val));
};

var dump_db = function(db, options) {
  return chan.each(
    function(data) { showPair(util.decode(data.key), data.value); },
    db.readRange(options));
};


var formatData = function(db, key) {
  return cc.go(function*() {
    var tmp = {};
    tmp[key] = (yield db.readEntity(key)) || null;
    return JSON.stringify(tmp, null, 2);
  });
};


var schema = {
  weight: {
    indexed: true
  },
  parent: {
    reference: true
  }
};


var show = function(db, dyn) {
  var entities = Array.prototype.slice.call(arguments, 2);

  return cc.go(function*() {
    for (var i = 0; i < entities.length; ++i)
      console.log(yield formatData(dyn, entities[i]));
    console.log();

    yield dump_db(db);
    console.log();
  });
};


cc.go(function*() {
  var db  = yield level('', { db: memdown });
  var dyn = yield curated(db, schema);

  yield cc.lift(Array,
                dyn.updateEntity('olaf', {
                  age: 50,
                  weight: { amount: 87.5, unit: 'kg' },
                  height: { amount: 187, unit: 'cm' }
                }),
                dyn.updateEntity('delaney', {
                  age: 5,
                  weight: { amount: 2.5, unit: 'kg' },
                  height: { amount: 25, unit: 'mm' },
                  parent: 'olaf'
                }),
                dyn.updateEntity('grace', {
                  age: 0,
                  weight: { amount: 30, unit: 'kg' },
                  height: { amount: 40, unit: 'cm' },
                  parent: 'olaf'
                }));

  yield show(db, dyn, 'olaf', 'delaney', 'grace');

  console.log('--- after changing grace\'s parent to delaney: ---');
  yield dyn.updateEntity('grace', { parent: 'delaney' });
  yield show(db, dyn, 'olaf', 'delaney', 'grace');

  console.log('--- after deleting delaney: ---');
  yield dyn.destroyEntity('delaney');
  yield show(db, dyn, 'olaf', 'delaney', 'grace');


  dyn.close();
});
