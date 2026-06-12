const dotenv = require("dotenv");
var dotex = require('dotenv-expand');

exports.load = function() {
    const conf = dotex.expand(dotenv.config());
    console.log("Configuration loaded.", conf);
}