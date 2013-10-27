# This proof-of-concept storage module for the Plexus ingest uses a sqlite3
# database as a simple key-value store.

Q   = require 'q'
fs  = require 'fs'
any = require 'any-db'


createTable = (db, name) ->
  Q.ninvoke db, "query",
    "CREATE TABLE #{name} (id text PRIMARY KEY, content text)"


readTable = (db, name, keys) ->
  if keys?
    keylist = keys.map((s) -> "'#{s}'").join(", ")
    query = "SELECT * FROM #{name} WHERE id IN (#{keylist})"
  else
    query = "SELECT * FROM #{name}"

  Q.ninvoke(db, "query", query)
    .then (response) ->
      result = {}
      for r in response.rows
        result[r.id] = JSON.parse(r.content)
      if keys?
        for k in keys
          result[k] ?= {}
      result


writeTable = (db, name, data) ->
  state = Q.fcall(->)

  for key, val of data then do (key, val) ->
    state = state.then ->
      Q.ninvoke db, "query",
        "INSERT OR REPLACE INTO #{name} VALUES (?, ?)",
        [key, JSON.stringify(val)]
  state


module.exports = (path, cb) ->
  withdb = (f) ->
    Q.nfcall(any.createConnection, "sqlite3://#{path}").then (db) ->
      f(db).fin(-> Q.ninvoke(db, 'end'))


  storage =
    readDependencyGraph: (cb) ->
      withdb((db) -> readTable(db, 'predecessors'))
      .nodeify(cb)

    writeDependencyGraph: (val, cb) ->
      withdb((db) -> writeTable(db, 'predecessors', val))
      .nodeify(cb)

    readSomeHeaders: (keys, cb) ->
      withdb((db) -> readTable(db, 'headers', keys))
      .nodeify(cb)

    writeSomeHeaders: (data, cb) ->
      withdb((db) -> writeTable(db, 'headers', data))
      .nodeify(cb)

    readSomeDeviations: (keys, cb) ->
      withdb((db) -> readTable(db, 'deviations', keys))
      .nodeify(cb)

    writeSomeDeviations: (data, cb) ->
      withdb((db) -> writeTable(db, 'deviations', data))
      .nodeify(cb)

    readSingleNodeDetails: (key, cb) ->
      withdb((db) -> readTable(db, 'details', [key]))
      .then((data) -> data[key])
      .nodeify(cb)

    writeSingleNodeDetails: (key, val, cb) ->
      data = {}
      data[key] = val
      withdb((db) -> writeTable(db, 'details', data))
      .nodeify(cb)

  fs.exists path, (exists) ->
    if exists
      cb(null, storage)
    else
      withdb (db) ->
        createTable(db, 'predecessors')
        .then(-> createTable(db, 'headers'))
        .then(-> createTable(db, 'details'))
        .then(-> createTable(db, 'deviations'))
      .then(-> cb(null, storage))
      .fail(cb)
