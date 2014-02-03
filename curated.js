'use strict';

var timestamp = require('monotonic-timestamp');

var cc   = require('ceci-core');
var chan = require('ceci-channels');
var cf   = require('ceci-filters');

var util = require('./util');


const END = '\xff';


var encode = function(key) {
  return util.encode(key);
};


var decode = function(key) {
  return util.decode(key);
};


module.exports = function(storage, indexedAttributes) {
  return cc.go(function*() {
    var indexed = indexedAttributes || {};

    var enqueueIndexAdditions = function(key, attr, batch) {
      for (var k in attr)
        if (indexed[k])
          batch.put(encode(['indx', k, attr[k], key]), ''); 
    };

    var enqueueIndexRemovals = function(key, attr, batch) {
      for (var k in attr)
        if (indexed[k])
          batch.del(encode(['indx', k, attr[k], key])); 
    };

    var writeAttributes = function(key, attr) {
      return cc.go(function*() {
        var t = timestamp().toString(36);
        var batch = storage.batch()
          .put(encode(['keys', key]), t)
          .put(encode(['attr', key]), attr)
          .put(encode(['hist', 'attr', key, t]), attr);

        enqueueIndexRemovals(key, yield readAttributes(key), batch);
        enqueueIndexAdditions(key, attr, batch);

        return yield batch.write();
      });
    };

    var readAttributes = function(key) {
      return storage.read(encode(['attr', key]));
    };

    var addRelation = function(pkey, ckey, value) {
      return cc.go(function*() {
        var t = timestamp().toString(36);
        var val = (value == undefined) ? true : value;

        return yield storage.batch()
          .put(encode(['succ', pkey, ckey]), val)
          .put(encode(['pred', ckey, pkey]), val)
          .put(encode(['hist', 'succ', pkey, ckey, t]), val)
          .put(encode(['hist', 'pred', ckey, pkey, t]), val)
          .write();
      });
    };

    var readRelatives = function(key, table) {
      return cf.map(
        function(item) { return decode(item.key)[2]; },
        storage.readRange({
          start: encode([table, key, '']),
          end  : encode([table, key, END])
        }));
    };

    var enqueueRelationRemoval = function(pkey, ckey, batch, timestamp) {
      batch.del(encode(['succ', pkey, ckey]));
      batch.put(encode(['hist', 'succ', pkey, ckey, timestamp]), false);
      batch.del(encode(['pred', ckey, pkey]));
      batch.put(encode(['hist', 'pred', ckey, pkey, timestamp]), false);
    };

    var destroy = function(key) {
      return cc.go(function*() {
        var t = timestamp().toString(36);

        var batch = storage.batch()
          .del(encode(['keys', key]))
          .del(encode(['attr', key]))
          .put(encode(['hist', 'attr', key, t]), null);

        yield chan.each(
          function(other) { enqueueRelationRemoval(key, other, batch, t); },
          readRelatives(key, 'succ'));

        yield chan.each(
          function(other) { enqueueRelationRemoval(other, key, batch, t); },
          readRelatives(key, 'pred'));
        
        enqueueIndexRemovals(key, yield readAttributes(key), batch);

        yield batch.write();
      });
    };

    return {
      writeAttributes: function(key, attr) {
        return writeAttributes(key, attr);
      },
      readAttributes: function(key) {
        return readAttributes(key);
      },
      addRelation: function(pkey, ckey) {
        return addRelation(pkey, ckey);
      },
      readSuccessors: function(key) {
        return readRelatives(key, 'succ');
      },
      readPredecessors: function(key) {
        return readRelatives(key, 'pred');
      },
      destroy: function(key) {
        return destroy(key);
      },
      close: storage.close
    };
  })
};
