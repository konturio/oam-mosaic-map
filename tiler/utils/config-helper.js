"use strict";
var path = require('path');
var pathToJson = path.resolve(__dirname, '../config.json');

var fs = require('fs'),
    config = JSON.parse(fs.readFileSync(pathToJson, 'utf8'));

function getConfig() {
    return config;
}

module.exports.getConfig = getConfig;