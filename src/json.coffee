Q = require 'q'
fs = require 'fs'
p = require 'path'
exec = require('child_process').exec


lazy = (f) ->
  val = undefined
  -> val ?= f()


readJSON = (path) ->
  deferred = Q.defer()

  fs.exists path, (exists) ->
    deferred.resolve(
      if exists
        Q.ninvoke(fs, 'readFile', path, { encoding: 'utf8' })
        .then(JSON.parse)
      else
        Q({})
    )

  deferred.promise


writeJSON = (path, val) ->
  Q.nfcall(exec, "mkdir -p '#{p.dirname(path)}'")
  .then ->
    text = JSON.stringify(val, null, 4)
    Q.ninvoke(fs, 'writeFile', path, text, { encoding: 'utf8' })


module.exports = (path, cb) ->
  cache =
    headers   : lazy -> readJSON("#{path}/headers.json")
    deviations: lazy -> readJSON("#{path}/deviations.json")

  read = (table, keys) ->
    cache[table]()
    .then((val) ->
      out = {}
      out[k] = (val[k] or {}) for k in keys
      out)

  write = (table, data) ->
    cache[table]()
    .then (val) ->
      val[k] = v for k, v of data
      writeJSON("#{path}/#{table}.json", val)

  detailsPath = (key) -> "#{path}/details/#{key[...6]}/#{key}.json"

  db =
    readDependencyGraph: (cb) ->
      readJSON("#{path}/graph.json")
      .nodeify(cb)

    writeDependencyGraph: (val, cb) ->
      writeJSON("#{path}/graph.json", val)
      .nodeify(cb)

    readSomeHeaders: (keys, cb) ->
      read('headers', keys)
      .nodeify(cb)

    writeSomeHeaders: (data, cb) ->
      write('headers', data)
      .nodeify(cb)

    readSomeDeviations: (keys, cb) ->
      read('deviations', keys)
      .nodeify(cb)

    writeSomeDeviations: (data, cb) ->
      write('deviations', data)
      .nodeify(cb)

    readSingleNodeDetails: (key, cb) ->
      readJSON(detailsPath(key))
      .nodeify(cb)

    writeSingleNodeDetails: (key, data, cb) ->
      writeJSON(detailsPath(key), data)
      .nodeify(cb)

  cb(null, db)
