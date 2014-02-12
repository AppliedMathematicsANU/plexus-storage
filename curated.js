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


var collated = function(input, getSchema) {
  return cc.go(function*() {
    var result = {};

    yield chan.each(
      function(item) {
        var key = item.key[0];
        var val = item.key[1];
        if (!getSchema(key).multiple)
          result[key] = val;
        else if (result[key])
          result[key].push(val);
        else
          result[key] = [val];
      },
      input);

    return result;
  });
};


var addLog = function(batch, time, entity, attr, op) {
  var vals = Array.prototype.slice.call(arguments, 5);
  batch.put(encode(['log', time, entity, attr, op].concat(vals)), '');
};


var removeDatum = function(batch, entity, attr, val, attrSchema, time, log) {
  if (log)
    addLog(batch, time, entity, attr, 'del', val);

  batch.del(encode(['eav', entity, attr, val]))
  batch.del(encode(['aev', attr, entity, val]));
  if (attrSchema.indexed)
    indexKeys(val, attrSchema.indexed).forEach(function(key) {
      batch.del(encode(['ave', attr, key, entity]));
    });
  if (attrSchema.reference)
    batch.del(encode(['vae', val, attr, entity]));
};


var removeData = function(batch, entity, attr, val, old, attrSchema, time) {
  if (attrSchema.multiple && Array.isArray(val))
    (Array.isArray(val) ? val : [val]).forEach(function(v) {
      if (!old || old.indexOf(v) >= 0)
        removeDatum(batch, entity, attr, v, attrSchema, time, true);
    });
  else
    removeDatum(batch, entity, attr, val, attrSchema, time, true);
};


var putDatum = function(batch, entity, attr, val, old, attrSchema, time) {
  if (old === undefined)
    addLog(batch, time, entity, attr, 'add', val);
  else {
    removeDatum(batch, entity, attr, old, attrSchema, time, false);
    addLog(batch, time, entity, attr, 'chg', old, val);
  }

  batch.put(encode(['eav', entity, attr, val]), time);
  batch.put(encode(['aev', attr, entity, val]), time);
  if (attrSchema.indexed)
    indexKeys(val, attrSchema.indexed).forEach(function(key) {
      batch.put(encode(['ave', attr, key, entity]), time);
    });
  if (attrSchema.reference)
    batch.put(encode(['vae', val, attr, entity]), time);
};


var putData = function(batch, entity, attr, val, old, attrSchema, time) {
  if (attrSchema.multiple) {
    (Array.isArray(val) ? val : [val]).forEach(function(v) {
      if (!old || old.indexOf(v) < 0)
        putDatum(batch, entity, attr, v, undefined, attrSchema, time);
    });
  } else {
    putDatum(batch, entity, attr, val, old, attrSchema, time);
  }
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
        return collated(scan(['eav', entity]), attrSchema);
      },

      updateEntity: function(entity, attr) {
        return atomically(function*(batch, time) {
          var old = yield this.byEntity(entity);
          for (var key in attr)
            putData(batch, entity, key,
                    attr[key], util.own(old, key), attrSchema(key), time);
        }.bind(this));
      },

      destroyEntity: function(entity) {
        return atomically(function*(batch, time) {
          var old = yield this.byEntity(entity);
          for (var key in old)
            removeData(batch, entity, key, old[key], null,
                       attrSchema(key), time);

          yield chan.each(
            function(item) {
              var attr = item.key[0];
              var other = item.key[1];
              removeData(batch, other, attr, entity, null,
                         attrSchema(attr), time);
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

          return yield collated(data, function(_) { return attrSchema(key); });
        });
      },

      updateAttribute: function(key, assign) {
        return atomically(function*(batch, time) {
          var old = yield this.byAttribute(key);
          for (var e in assign)
            putData(batch, e, key,
                    assign[e], util.own(old, e), attrSchema(key), time);
        }.bind(this));
      },

      destroyAttribute: function(key) {
        return atomically(function*(batch, time) {
          var old = yield this.byAttribute(key);
          for (var e in old)
            removeData(batch, e, key, old[e], null, attrSchema(key), time);
        }.bind(this));
      },

      unlist: function(entity, attribute, values) {
        return atomically(function*(batch, time) {
          var old = yield this.byEntity(entity);
          removeData(batch, entity, attribute, values, util.own(old, attribute),
                     attrSchema(attribute), time);
        }.bind(this));
      }
    };
  })
};
