"use strict";
const path = require('path');
const pathToJson = path.resolve(__dirname, '../config.json');

const fs = require('fs'),
    config = JSON.parse(fs.readFileSync(pathToJson, 'utf8'));

function getConfig() {
    return config;
}

module.exports.getConfig = getConfig;