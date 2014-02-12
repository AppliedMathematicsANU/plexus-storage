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
    tmp[key] = (yield db.byEntity(key)) || null;
    return JSON.stringify(tmp, null, 2);
  });
};

var formatAttribute = function(db, key) {
  return cc.go(function*() {
    var tmp = {};
    tmp[key] = (yield db.byAttribute(key)) || null;
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
  blurb: {
    indexed: function(text) { return text.trim().split(/\s*\b/); }
  },
  weight: {
    indexed: true
  },
  parents: {
    reference: true,
    multiple : true
  }
};


top(function*() {
  var db  = yield level('', { db: memdown });
  var dyn = yield curated(db, schema);
  var entities = ['olaf', 'delaney', 'grace'];
  var attributes = ['greeting', 'age', 'weight', 'height', 'parents'];

  yield cc.lift(Array)(
    dyn.updateEntity('olaf', {
      greeting: 'Hello, I am Olaf!',
      age     : 50,
      weight  : 87.5,
      height  : 187.0
    }),
    dyn.updateEntity('delaney', {
      greeting: 'Hi there.',
      age     : 5,
      weight  : 2.5,
      height  : 2.5,
      parents : 'olaf'
    }),
    dyn.updateEntity('grace', {
      greeting: 'Nice to meet you!',
      age     : 0,
      weight  : 30,
      height  : 40,
      parents : 'olaf'
    }));

  yield show(db, dyn, entities, attributes);

  console.log('weights between 20 and 50:',
              yield dyn.byAttribute('weight', { from: 20, to: 50 }));
  console.log('heights between 0 and 50:',
              yield dyn.byAttribute('height', { from: 0, to: 50 }));
  console.log('words starting with H in greetings',
              yield dyn.byAttribute('greeting', { from: 'H', to: 'H~' }));
  console.log();

  console.log('--- after add olaf and delaney to grace\'s parent: ---');
  yield dyn.updateEntity('grace', { parents: ['olaf', 'delaney'] });
  yield show(db, dyn, entities, attributes);

  console.log('--- after changing olaf\'s weight: ---');
  yield dyn.updateAttribute('weight', { olaf: 86 });
  yield show(db, dyn, entities, attributes);

  console.log('--- after deleting delaney: ---');
  yield dyn.destroyEntity('delaney');
  yield show(db, dyn, entities, attributes);

  console.log('--- after deleting weights: ---');
  yield dyn.destroyAttribute('weight');
  yield show(db, dyn, entities, attributes);

  dyn.close();
});
