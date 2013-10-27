Q = require 'q'
fs = require 'fs'


printNicely = (data) ->
  keys = (k for k of data.predecessors)
  keys.sort()

  for k in keys
    entry =
      predecessors: data.predecessors[k]
      headers     : data.headers[k]
      deviations  : data.deviations[k]
      details     : data.details[k]

    console.log("#{k}:")
    console.log(JSON.stringify(entry, null, 4))
    console.log()


if module? and not module.parent
  input_type = process.argv[2]
  input_path = process.argv[3]

  input_storage = require("./#{input_type}")

  Q.longStackSupport = true

  data = {}

  Q.nfcall(input_storage, input_path).then (db) ->
    Q.ninvoke(db, 'readDependencyGraph')
    .then((val) -> data.predecessors = val)
    .then(-> Q.ninvoke(db, 'readSomeHeaders', k for k of data.predecessors))
    .then((val) -> data.headers = val)
    .then(-> Q.ninvoke(db, 'readSomeDeviations', k for k of data.predecessors))
    .then((val) -> data.deviations = val)
    .then(->
      data.details = {}
      (for k of data.predecessors then do (k) ->
        -> Q.ninvoke(db, 'readSingleNodeDetails', k)
           .then((val) -> data.details[k] = val))
      .reduce(Q.when, Q()))
    .then(-> printNicely(data))
    .done()
