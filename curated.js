'use strict';

var cc   = require('ceci-core');
var chan = require('ceci-channels');
var cf   = require('ceci-filters');

var util = require('./util');

var encode = util.encode;
var decode = util.decode;


var putDatum = function(batch, entity, attr, val, attrSchema, timestamp) {
  batch.put(encode(['eav', entity, attr, val]), timestamp);
  batch.put(encode(['aev', attr, entity, val]), timestamp);
  if (attrSchema.indexed)
    batch.put(encode(['ave', attr, val, entity]), timestamp);
  if (attrSchema.reference)
    batch.put(encode(['vae', val, attr, entity]), timestamp);
};


var removeDatum = function(batch, entity, attr, val, attrSchema) {
  batch.del(encode(['eav', entity, attr, val]))
  batch.del(encode(['aev', attr, entity, val]));
  if (attrSchema.indexed)
    batch.del(encode(['ave', attr, val, entity]));
  if (attrSchema.reference)
    batch.del(encode(['vae', val, attr, entity]));
};


var addReverseLog = function(batch, entity, attr, val, timestamp) {
  batch.put(encode(['rev', timestamp, entity, attr]), val);
};


module.exports = function(storage, schema) {
  schema = schema || {};

  return cc.go(function*() {
    var attrSchema = function(key) {
      return schema[key] || {};
    };

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
          start: encode(given.concat(null)),
          end  : encode(given.concat(undefined))
        }));
    };

    var makeTimestamp = function() {
      return cc.go(function*() {
        var t = yield chan.pull(scan('seq'));
        var next = (t === undefined) ? -1 : t.key[0] - 1;
        yield storage.write(encode(['seq', next]), Date.now());
        return next;
      });
    };

    var readEntity = function(entity) {
      return cc.go(function*() {
        var result = {};

        yield chan.each(
          function(item) { result[item.key[0]] = item.key[1]; },
          scan('eav', entity));

        return result;
      });
    };

    var updateEntity = function(entity, attr) {
      return cc.go(function*() {
        var t = yield makeTimestamp();
        var batch = storage.batch();
        var key;

        var old = yield readEntity(entity);
        for (key in old)
          if (attr.hasOwnProperty(key))
            removeDatum(batch, entity, key, old[key], attrSchema(key));

        for (key in attr) {
          putDatum(batch, entity, key, attr[key], attrSchema(key), t);
          if (old.hasOwnProperty(key))
            addReverseLog(batch, entity, key, [old[key]], t);
          else
            addReverseLog(batch, entity, key, [], t);
        }

        return yield batch.write();
      });
    };

    var destroyEntity = function(entity) {
      return cc.go(function*() {
        var t = yield makeTimestamp();
        var batch = storage.batch();

        var old = yield readEntity(entity);
        for (var key in old) {
          removeDatum(batch, entity, key, old[key], attrSchema(key));
          addReverseLog(batch, entity, key, [old[key]], t);
        }

        yield chan.each(
          function(item) {
            var attr = item.key[0];
            var other = item.key[1];
            removeDatum(batch, other, attr, entity, attrSchema(attr));
            addReverseLog(batch, other, attr, [entity], t);
          },
          scan('vae', entity));

        yield batch.write();
      });
    };

    return {
      updateEntity: function(entity, attr) {
        return updateEntity(entity, attr);
      },
      readEntity: function(entity) {
        return readEntity(entity);
      },
      destroyEntity: function(entity) {
        return destroyEntity(entity);
      },
      close: storage.close
    };
  })
};
