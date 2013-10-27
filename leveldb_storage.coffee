Q     = require 'q'
fs    = require 'fs'
level = require 'level'


readDB = (db, key) ->
  deferred = Q.defer()

  db.get key, (err, val) ->
    if not err
      deferred.resolve(val)
    else if err.notFound
      deferred.resolve({})
    else
      deferred.reject(err)

  deferred.promise


writeDB = (db, key, val) -> Q.ninvoke(db, 'put', key, val)

mapHash = (h, f) -> f(key, val) for key, val of h

zipmap = (keys, values) ->
  result = {}
  for i in [0...Math.min(keys.length, values.length)]
    result[keys[i]] = values[i]
  result


module.exports = (path, cb) ->
  withdb = (f) ->
    Q.nfcall(level, path, { valueEncoding: 'json' })
    .then (db) -> f(db).fin(-> Q.ninvoke(db, 'close'))

  db =
    readDependencyGraph: (cb) ->
      withdb((db) -> readDB(db, 'predecessors'))
      .nodeify(cb)

    writeDependencyGraph: (val, cb) ->
      withdb((db) -> writeDB(db, 'predecessors', val))
      .nodeify(cb)

    readSomeHeaders: (keys, cb) ->
      withdb((db) ->
        Q.all(keys.map((key) -> readDB(db, "headers-#{key}")))
        .then((values) -> zipmap(keys, values)))
      .nodeify(cb)

    writeSomeHeaders: (data, cb) ->
      withdb((db) ->
        mapHash(data, (key, val) -> -> writeDB(db, "headers-#{key}", val))
        .reduce(Q.when, Q()))
      .nodeify(cb)

    readSomeDeviations: (keys, cb) ->
      withdb((db) ->
        Q.all(keys.map((key) -> readDB(db, "deviations-#{key}")))
        .then((values) -> zipmap(keys, values)))
      .nodeify(cb)

    writeSomeDeviations: (data, cb) ->
      withdb((db) ->
        mapHash(data, (key, val) -> -> writeDB(db, "deviations-#{key}", val))
        .reduce(Q.when, Q()))
      .nodeify(cb)

    readSingleNodeDetails: (key, cb) ->
      withdb((db) -> readDB(db, "details-#{key}"))
      .nodeify(cb)

    writeSingleNodeDetails: (key, data, cb) ->
      withdb((db) -> writeDB(db, "details-#{key}", data))
      .nodeify(cb)

  cb(null, db)
