'use strict';

var cc   = require('ceci-core');
var chan = require('ceci-channels');
var cf   = require('ceci-filters');

var util = require('./util');

var encode = util.encode;
var decode = util.decode;


var addReverseLog = function(batch, entity, attr, val, time) {
  batch.put(encode(['rev', time, entity, attr]), val);
};


var removeDatum = function(batch, entity, attr, val, attrSchema, time) {
  addReverseLog(batch, entity, attr, val, time);

  batch.del(encode(['eav', entity, attr, val]))
  batch.del(encode(['aev', attr, entity, val]));
  if (attrSchema.indexed)
    batch.del(encode(['ave', attr, val, entity]));
  if (attrSchema.reference)
    batch.del(encode(['vae', val, attr, entity]));
};


var putDatum = function(batch, entity, attr, val, old, attrSchema, time) {
  if (old === undefined)
    addReverseLog(batch, entity, attr, [], time);
  else
    removeDatum(batch, entity, attr, old, attrSchema, time);

  batch.put(encode(['eav', entity, attr, val]), time);
  batch.put(encode(['aev', attr, entity, val]), time);
  if (attrSchema.indexed)
    batch.put(encode(['ave', attr, val, entity]), time);
  if (attrSchema.reference)
    batch.put(encode(['vae', val, attr, entity]), time);
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

    var nextTimestamp = function(batch) {
      return cc.go(function*() {
        var t = yield chan.pull(scan('seq'));
        var next = (t === undefined) ? -1 : t.key[0] - 1;
        batch.put(encode(['seq', next]), Date.now());
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
        var batch = storage.batch();
        var t = yield nextTimestamp(batch);
        var old = yield readEntity(entity);
        var key;

        for (key in attr)
          putDatum(batch, entity, key,
                   attr[key], util.own(old, key), attrSchema(key), t);

        return yield batch.write();
      });
    };

    var destroyEntity = function(entity) {
      return cc.go(function*() {
        var batch = storage.batch();
        var t = yield nextTimestamp(batch);

        var old = yield readEntity(entity);
        for (var key in old)
          removeDatum(batch, entity, key, old[key], attrSchema(key), t);

        yield chan.each(
          function(item) {
            var attr = item.key[0];
            var other = item.key[1];
            removeDatum(batch, other, attr, entity, attrSchema(attr), t);
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
