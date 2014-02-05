'use strict';

var timestamp = require('monotonic-timestamp');

var cc   = require('ceci-core');
var chan = require('ceci-channels');
var cf   = require('ceci-filters');

var util = require('./util');

var encode = util.encode;
var decode = util.decode;

const END = '\xff';


var putAttr = function(batch, entity, attr, val, attrSchema) {
  batch.put(encode(['eav', entity, attr, val]), '');
  batch.put(encode(['aev', attr, entity, val]), '');
  if (attrSchema.indexed)
    batch.put(encode(['ave', attr, val, entity]), '');
  if (attrSchema.reference)
    batch.put(encode(['vae', val, attr, entity]), '');
};


var delAttr = function(batch, entity, attr, val, attrSchema) {
  batch.del(encode(['eav', entity, attr, val]))
  batch.del(encode(['aev', attr, entity, val]));
  if (attrSchema.indexed)
    batch.del(encode(['ave', attr, val, entity]));
  if (attrSchema.reference)
    batch.del(encode(['vae', val, attr, entity]), '');
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

    var readAttributes = function(entity) {
      return cc.go(function*() {
        var result = {};

        yield chan.each(
          function(item) { result[item.key[0]] = item.key[1]; },
          scan('eav', entity));

        return result;
      });
    };

    var writeAttributes = function(entity, attr) {
      return cc.go(function*() {
        var batch = storage.batch();
        var key;

        var old = yield readAttributes(entity);
        for (key in old)
          if (attr.hasOwnProperty(key))
            delAttr(batch, entity, key, old[key], attrSchema(key));

        for (key in attr)
          putAttr(batch, entity, key, attr[key], attrSchema(key));

        return yield batch.write();
      });
    };

    var destroy = function(entity) {
      return cc.go(function*() {
        var batch = storage.batch();

        var old = yield readAttributes(entity);
        for (var key in old)
          delAttr(batch, entity, key, old[key], attrSchema(key));

        yield chan.each(
          function(item) {
            var attr = item.key[0];
            var other = item.key[1];
            delAttr(batch, other, attr, entity, attrSchema(attr));
          },
          scan('vae', entity));

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
      destroy: function(entity) {
        return destroy(entity);
      },
      close: storage.close
    };
  })
};
