/*jshint esversion: 6 */
module.exports = function(settings) {
	  var module = {};
	  module.player = require("./lib/playercache.js")(settings);
	  module.tribe = require("./lib/tribecache.js")(settings);
	  
	  return module;
};
