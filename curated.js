'use strict';

var timestamp = require('monotonic-timestamp');

var cc   = require('ceci-core');
var chan = require('ceci-channels');
var cf   = require('ceci-filters');

var util = require('./util');


const SEP = '\x00';
const END = '\xff';


module.exports = function(storage) {
  return cc.go(function*() {
    var path = function() {
      var args = Array.prototype.slice.call(arguments);
      return args.join(SEP);
    };

    var writeAttributes = function(key, attr) {
      return cc.go(function*() {
        var t = timestamp().toString(36);

        return yield storage.batch()
          .put(path('keys', key), t)
          .put(path('attr', key), attr)
          .put(path('hist', 'attr', key, t), attr)
          .write();
      });
    };

    var readAttributes = function(key) {
      return storage.read(path('attr', key));
    };

    var addRelation = function(pkey, ckey, value) {
      return cc.go(function*() {
        var t = timestamp().toString(36);
        var val = (value == undefined) ? true : value;

        return yield storage.batch()
          .put(path('succ', pkey, ckey), val)
          .put(path('pred', ckey, pkey), val)
          .put(path('hist', 'succ', pkey, ckey, t), val)
          .put(path('hist', 'pred', ckey, pkey, t), val)
          .write();
      });
    };

    var readRelatives = function(key, table) {
      return cf.map(
        function(item) { return item.key.split(SEP)[2]; },
        storage.readRange({
          start: path(table, key, ''),
          end  : path(table, key, END)
        }));
    };

    var scheduleRelationRemoval = function(pkey, ckey, batch, timestamp) {
      batch.del(path('succ', pkey, ckey));
      batch.put(path('hist', 'succ', pkey, ckey, timestamp), false);
      batch.del(path('pred', ckey, pkey));
      batch.put(path('hist', 'pred', ckey, pkey, timestamp), false);
    };

    var destroy = function(key) {
      return cc.go(function*() {
        var t = timestamp().toString(36);

        var batch = storage.batch()
          .del(path('keys', key))
          .del(path('attr', key))
          .put(path('hist', 'attr', key, t), null);

        yield chan.each(
          function(other) { scheduleRelationRemoval(key, other, batch, t); },
          readRelatives(key, 'succ'));

        yield chan.each(
          function(other) { scheduleRelationRemoval(other, key, batch, t); },
          readRelatives(key, 'pred'));
        
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
