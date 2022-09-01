const express = require("express");
const router = express.Router();
const config = require("../utils/config-helper").getConfig();
const fs = require("fs");
const pg = require("pg"),
    pool = new pg.Pool(config.pg);
const path = require('path');
const http = require('http');

let client, resl, fileExists;
const live = { "status": "UP" };
const unLive = { "status": "DOWN" }
router.get("/live", async (req, res) => {
    //check db && file exist
    try {
        //check db
        client = await pool.connect();
        resl = await client.query("select 1 as resl");

        //check file exist
        var oam_path = path.resolve(__dirname, "../" + config.app.oam_path);
        fileExists = await fs.promises.readdir(oam_path)
            .then(files => {
                return files.length !== 0;
            })
            .catch(err => {
                res.json(unLive);
                throw err;
            })
    
        if (fileExists && resl.rows && resl.rows.length > 0 && resl.rows[0].resl === 1) { 
            res.json(live); 
        } else {
            res.json(unLive);
        }

    } catch (err) {
        res.json(unLive);
        throw err;
    }
    finally {
        client.release();
    }
});

router.get("/ready", async (req, res) => {
    //check localhost
    try {
        http.request({
            port: config.app.port,
            host: 'localhost',
            method: 'GET',
            path: '/'
        });
        res.json(live);
    } catch (err) {
        res.json(unLive);
        throw err;
    }
});

module.exports = router;