'use strict';

var cc   = require('ceci-core');
var chan = require('ceci-channels');
var cf   = require('ceci-filters');

var util = require('./util');

var encode = util.encode;
var decode = util.decode;


var indexKeys = function(value, indexer) {
  if (typeof indexer == 'function')
    return indexer(value);
  else
    return [value];
};


var addReverseLog = function(batch, entity, attr, val, time) {
  batch.put(encode(['rev', time, entity, attr]), val);
};


var collate = function(input, getSchema) {
  return cc.go(function*() {
    var result = {};

    yield chan.each(
      function(item) {
        result[item.key[0]] = item.key[1];
      },
      input);

    return result;
  });
};


var removeDatum = function(batch, entity, attr, val, attrSchema, time) {
  addReverseLog(batch, entity, attr, [val], time);

  batch.del(encode(['eav', entity, attr, val]))
  batch.del(encode(['aev', attr, entity, val]));
  if (attrSchema.indexed)
    indexKeys(val, attrSchema.indexed).forEach(function(key) {
      batch.del(encode(['ave', attr, key, entity]));
    });
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
    indexKeys(val, attrSchema.indexed).forEach(function(key) {
      batch.put(encode(['ave', attr, key, entity]), time);
    });
  if (attrSchema.reference)
    batch.put(encode(['vae', val, attr, entity]), time);
};


module.exports = function(storage, schema) {
  schema = schema || {};

  return cc.go(function*() {
    var lock = chan.createLock();

    var scan = function(prefix, range, limit) {
      var n = prefix.length;
      var from = range ? range.from : null;
      var to   = range ? range.to : undefined;

      return cf.map(
        function(item) {
          return {
            key  : decode(item.key).slice(n),
            value: item.value
          }
        },
        storage.readRange({
          start: encode(prefix.concat(from)),
          end  : encode(prefix.concat(to)),
          limit: (limit == null ? -1 : limit)
        }));
    };

    var nextTimestamp = function(batch) {
      return cc.go(function*() {
        var t = yield chan.pull(scan(['seq'], null, 1));
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

    return {
      close: storage.close,

      byEntity: function(entity) {
        return collate(scan(['eav', entity]), attrSchema);
      },

      updateEntity: function(entity, attr) {
        return atomically(function*(batch, time) {
          var old = yield this.byEntity(entity);
          for (var key in attr)
            putDatum(batch, entity, key,
                     attr[key], util.own(old, key), attrSchema(key), time);
        }.bind(this));
      },

      destroyEntity: function(entity) {
        return atomically(function*(batch, time) {
          var old = yield this.byEntity(entity);
          for (var key in old)
            removeDatum(batch, entity, key, old[key], attrSchema(key), time);

          yield chan.each(
            function(item) {
              var attr = item.key[0];
              var other = item.key[1];
              removeDatum(batch, other, attr, entity, attrSchema(attr), time);
            },
            scan(['vae', entity]));
        }.bind(this));
      },

      byAttribute: function(key, range) {
        return cc.go(function*() {
          var data;
          if (range) {
            if (attrSchema(key).indexed)
              data = cf.map(
                function(item) {
                  return {
                    key  : [item.key[1], item.key[0]],
                    value: item.value
                  }
                },
                scan(['ave', key], range));
            else
              data = cf.filter(
                function(item) {
                  var val = item.key[1];
                  return val >= range.from && val <= range.to;
                },
                scan(['aev', key]));
          }
          else
            data = scan(['aev', key]);

          return yield collate(data, function(_) { return attrSchema(key); });
        });
      },

      updateAttribute: function(key, assign) {
        return atomically(function*(batch, time) {
          var old = yield this.byAttribute(key);
          for (var e in assign)
            putDatum(batch, e, key,
                     assign[e], util.own(old, e), attrSchema(key), time);
        }.bind(this));
      },

      destroyAttribute: function(key) {
        return atomically(function*(batch, time) {
          var old = yield this.byAttribute(key);
          for (var e in old)
            removeDatum(batch, e, key, old[e], attrSchema(key), time);
        }.bind(this));
      }
    };
  })
};
