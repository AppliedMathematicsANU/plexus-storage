'use strict';

var cc   = require('ceci-core');
var chan = require('ceci-channels');
var cf   = require('ceci-filters');

var util = require('./util');

var encode = util.encode;
var decode = util.decode;


function Lock() {
  this.busy = chan.chan();
  this.release();
};

Lock.prototype = {
  acquire: function() {
    return chan.pull(this.busy);
  },
  release: function() {
    chan.push(this.busy, null);
  }
};


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
    var lock = new Lock();

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

    var atomically = function(action) {
      return cc.go(function*() {
        yield lock.acquire();
        var batch = storage.batch();
        yield cc.go(action, batch, yield nextTimestamp(batch));
        yield batch.write();
        lock.release();
      });
    };

    var attrSchema = function(key) {
      return schema[key] || {};
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
      return atomically(function*(batch, time) {
        var old = yield readEntity(entity);
        for (var key in attr)
          putDatum(batch, entity, key,
                   attr[key], util.own(old, key), attrSchema(key), time);
      });
    };

    var destroyEntity = function(entity) {
      return atomically(function*(batch, time) {
        var old = yield readEntity(entity);
        for (var key in old)
          removeDatum(batch, entity, key, old[key], attrSchema(key), time);

        yield chan.each(
          function(item) {
            var attr = item.key[0];
            var other = item.key[1];
            removeDatum(batch, other, attr, entity, attrSchema(attr), time);
          },
          scan('vae', entity));
      });
    };

    var readAttribute = function(key) {
      return cc.go(function*() {
        var result = {};

        yield chan.each(
          function(item) { result[item.key[0]] = item.key[1]; },
          scan('aev', key));

        return result;
      });
    };

    var updateAttribute = function(key, assign) {
      return atomically(function*(batch, time) {
        var old = yield readAttribute(key);
        for (var e in assign)
          putDatum(batch, e, key,
                   assign[e], util.own(old, e), attrSchema(key), time);
      });
    };

    var destroyAttribute = function(key) {
      return atomically(function*(batch, time) {
        var old = yield readAttribute(key);
        for (var e in old)
          removeDatum(batch, e, key, old[e], attrSchema(key), time);
      });
    };

    return {
      updateEntity    : updateEntity,
      readEntity      : readEntity,
      destroyEntity   : destroyEntity,
      updateAttribute : updateAttribute,
      readAttribute   : readAttribute,
      destroyAttribute: destroyAttribute,
      close           : storage.close
    };
  })
};
