'use strict';

var cc = require('ceci-core');
var auto = require('./autoingests');


var printNicely = function(data) {
  var keys = Object.keys(data.predecessors);
  keys.sort();

  keys.forEach(function(k) {
    var entry = {
      predecessors: data.predecessors[k],
      headers     : data.headers[k],
      deviations  : data.deviations[k],
      details     : data.details[k]
    };
    console.log(k + ":");
    console.log(JSON.stringify(entry, null, 4));
    console.log();
  });
};


if (module != null && !module.parent) {
  var input_type = process.argv[2];
  var input_path = process.argv[3];
  var data = {};

  cc.go(function*() {
    var db = yield auto(input_path, require("./" + input_type));
    var keys, i, k;

    data.predecessors = yield db.readDependencyGraph();
    keys = Object.keys(data.predecessors);

    data.headers = yield db.readSomeHeaders(keys);
    data.deviations = yield db.readSomeDeviations(keys);

    data.details = {};
    for (i = 0; i < keys.length; ++i) {
      k = keys[i];
      data.details[k] = yield db.readSingleNodeDetails(k);
    }

    printNicely(data);
  });
}
