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


var formatEntity = function(db, key) {
  return cc.go(function*() {
    var tmp = {};
    tmp[key] = (yield db.byEntity(key)) || null;
    tmp[key].references = (yield db.references(key)) || null;
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


var show = function(rawDB, db, entities, attributes) {
  return cc.go(function*() {
    for (var i = 0; i < entities.length; ++i)
      console.log(yield formatEntity(db, entities[i]));
    console.log();

    for (var i = 0; i < attributes.length; ++i)
      console.log(yield formatAttribute(db, attributes[i]));
    console.log();

    yield chan.each(
      function(data) {
        console.log(JSON.stringify(util.decode(data.key)) + ':',
                    JSON.stringify(data.value));
      },
      rawDB.readRange());

    console.log();
  });
};


var schema = {
  greeting: {
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
  var rawDB  = yield level('', { db: memdown });
  var db = yield curated(rawDB, schema);
  var entities = ['olaf', 'delaney', 'grace'];
  var attributes = ['greeting', 'age', 'weight', 'height', 'parents'];

  yield cc.lift(Array)(
    db.updateEntity('olaf', {
      greeting: 'Hello, I am Olaf!',
      age     : 50,
      weight  : 87.5,
      height  : 187.0
    }),
    db.updateEntity('delaney', {
      greeting: 'Hi there.',
      age     : 5,
      weight  : 2.5,
      height  : 2.5,
      parents : 'olaf'
    }),
    db.updateEntity('grace', {
      greeting: 'Nice to meet you!',
      age     : 0,
      weight  : 30,
      height  : 40,
      parents : 'olaf'
    }));

  yield show(rawDB, db, entities, attributes);

  console.log('weights between 20 and 50:',
              yield db.byAttribute('weight', { from: 20, to: 50 }));
  console.log('heights between 0 and 50:',
              yield db.byAttribute('height', { from: 0, to: 50 }));
  console.log('words starting with H in greetings',
              yield db.byAttribute('greeting', { from: 'H', to: 'H~' }));
  console.log();

  console.log('--- after add olaf and delaney to grace\'s parent: ---');
  yield db.updateEntity('grace', { parents: ['olaf', 'delaney'] });
  yield show(rawDB, db, entities, attributes);

  console.log('--- after changing olaf\'s weight: ---');
  yield db.updateAttribute('weight', { olaf: 86 });
  yield show(rawDB, db, entities, attributes);

  console.log('--- after deleting delaney: ---');
  yield db.destroyEntity('delaney');
  yield show(rawDB, db, entities, attributes);

  console.log('--- after deleting weights: ---');
  yield db.destroyAttribute('weight');
  yield show(rawDB, db, entities, attributes);

  console.log('--- after unlisting olaf and delaney as grace\'s parents: ---');
  yield db.unlist('grace', 'parents', ['olaf', 'delaney']);
  yield show(rawDB, db, entities, attributes);

  db.close();
});
