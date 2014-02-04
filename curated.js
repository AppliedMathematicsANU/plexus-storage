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


var removeRelation = function(batch, parent, child) {
  batch.del(encode(['succ', parent, child]));
  batch.del(encode(['pred', child, parent]));
};


var setAttribute = function(batch, entity, attr, oldval, newval, indexed) {
  return cc.go(function*() {
    batch.put(encode(['eav', entity, attr, newval]), '');
    batch.put(encode(['aev', attr, entity, newval]), '');
    if (indexed) {
      batch.del(encode(['ave', attr, oldval, entity]));
      batch.put(encode(['ave', attr, newval, entity]), '');
    }
  });
};


var deleteAttribute = function(batch, entity, attr, oldval, indexed) {
  return cc.go(function*() {
    batch.del(encode(['eav', entity, attr, oldval]))
    batch.del(encode(['aev', attr, entity, oldval]));
    if (indexed)
      batch.del(encode(['ave', attr, oldval, entity]));
  });
};


module.exports = function(storage, indexedAttributes) {
  return cc.go(function*() {
    var indexed = indexedAttributes || {};

    var scan = function() {
      var given = Array.prototype.slice.call(arguments);
      var n = given.length;

      return cf.map(
        function(item) {
          return {
            key  : decode(item.key).slice(n),
            value: item.value
          }
        },
        storage.readRange({
          start: encode(given.concat('')),
          end  : encode(given.concat(END))
        }));
    };

    var getAttribute = function(entity, attr) {
      return storage.read(encode(['eav', entity, attr]));
    };

    var readAttributes = function(entity) {
      return cc.go(function*() {
        var result = {};

        yield chan.each(
          function(item) { result[item.key[0]] = item.key[1]; },
          scan('eav', entity));

        return result;
      });
    };

    var readRelatives = function(entity, table) {
      return cf.map(
        function(item) { return item.key[0]; },
        scan(table, entity));
    };

    var writeAttributes = function(entity, attr) {
      return cc.go(function*() {
        var old = yield readAttributes(entity);
        var batch = storage.batch();
        var key;

        for (key in old)
          yield deleteAttribute(batch, entity, key, old[key], indexed[key]);

        for (key in attr)
          yield setAttribute(
            batch, entity, key, old[key], attr[key], indexed[key]);

        return yield batch.write();
      });
    };

    var addRelation = function(parent, child, value) {
      return cc.go(function*() {
        var val = (value == undefined) ? true : value;

        return yield storage.batch()
          .put(encode(['succ', parent, child]), val)
          .put(encode(['pred', child, parent]), val)
          .write();
      });
    };

    var destroy = function(entity) {
      return cc.go(function*() {
        var batch = storage.batch().del(encode(['attr', entity]));

        var old = yield readAttributes(entity);
        for (var key in old)
          yield deleteAttribute(batch, entity, key, old[key], indexed[key]);

        yield chan.each(
          function(other) { removeRelation(batch, entity, other); },
          readRelatives(entity, 'succ'));

        yield chan.each(
          function(other) { removeRelation(batch, other, entity); },
          readRelatives(entity, 'pred'));

        yield batch.write();
      });
    };

    return {
      writeAttributes: function(entity, attr) {
        return writeAttributes(entity, attr);
      },
      readAttributes: function(entity) {
        return readAttributes(entity);
      },
      addRelation: function(parent, child) {
        return addRelation(parent, child);
      },
      readSuccessors: function(entity) {
        return readRelatives(entity, 'succ');
      },
      readPredecessors: function(entity) {
        return readRelatives(entity, 'pred');
      },
      destroy: function(entity) {
        return destroy(entity);
      },
      close: storage.close
    };
  })
};
