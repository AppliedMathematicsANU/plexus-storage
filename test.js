'use strict';

var memdown  = require('memdown');

var cc   = require('ceci-core');
var chan = require('ceci-channels');

var level   = require('./leveldb');
var curated = require('./curated');
var util    = require('./util');


var top = function(gen) {
  cc.go(gen).then(null, function(ex) { console.log(ex.stack); });
};


var showPair = function(key, val) {
  console.log(JSON.stringify(key) + ': ' + JSON.stringify(val));
};

var dump_db = function(db, options) {
  return chan.each(
    function(data) { showPair(util.decode(data.key), data.value); },
    db.readRange(options));
};


var formatEntity = function(db, key) {
  return cc.go(function*() {
    var tmp = {};
    tmp[key] = (yield db.readEntity(key)) || null;
    return JSON.stringify(tmp, null, 2);
  });
};

var formatAttribute = function(db, key) {
  return cc.go(function*() {
    var tmp = {};
    tmp[key] = (yield db.readAttribute(key)) || null;
    return JSON.stringify(tmp, null, 2);
  });
};

var show = function(db, dyn, entities, attributes) {
  return cc.go(function*() {
    for (var i = 0; i < entities.length; ++i)
      console.log(yield formatEntity(dyn, entities[i]));
    console.log();

    for (var i = 0; i < attributes.length; ++i)
      console.log(yield formatAttribute(dyn, attributes[i]));
    console.log();

    yield dump_db(db);
    console.log();
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


top(function*() {
  var db  = yield level('', { db: memdown });
  var dyn = yield curated(db, schema);
  var entities = ['olaf', 'delaney', 'grace'];
  var attributes = ['age', 'weight', 'height', 'parent'];

  yield cc.lift(Array)(
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

  yield show(db, dyn, entities, attributes);

  console.log('--- after changing grace\'s parent to delaney: ---');
  yield dyn.updateEntity('grace', { parent: 'delaney' });
  yield show(db, dyn, entities, attributes);

  console.log('--- after changing olaf\'s weight: ---');
  yield dyn.updateAttribute('weight', { olaf: { amount: 86, unit: 'kg' } });
  yield show(db, dyn, entities, attributes);

  console.log('--- after deleting delaney: ---');
  yield dyn.destroyEntity('delaney');
  yield show(db, dyn, entities, attributes);

  console.log('--- after deleting weights: ---');
  yield dyn.destroyAttribute('weight');
  yield show(db, dyn, entities, attributes);

  dyn.close();
});
