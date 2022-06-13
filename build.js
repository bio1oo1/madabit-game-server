var NodeUglifier = require('node-uglifier');
var nodeUglifier = new NodeUglifier('server.js');

nodeUglifier.merge().uglify();
nodeUglifier.exportToFile('./build/server.min.js');
