const express = require('express');
const router = express.Router();
const config = require('../utils/config-helper').getConfig();
const { spawn } = require('child_process');
const fs = require("fs");
const pg = require('pg'),
    pool = new pg.Pool(config.pg);

const BASE_URL = config.app.base_url;
const REOR_COG_PATH = "/home/gis/rastertiler/reor_colored_cog.tif";

router.get("/tilejson.json", async (req, res) => {
    res.json({
        tilejson: "2.2.0",
        version: "1.0.0",
        scheme: "xyz",
        tiles: [`${BASE_URL}/tiles/reor/{z}/{x}/{y}.png`],
        minzoom: 11,
        maxzoom: 20,
        center: [-95.1649143756602, 29.73059311839303, 13],
    });
});

router.get("/:z/:x/:y.png", async (req, res) => {
    const { z, x, y } = req.params;

    const { rows } = await client.query(`
      with tile as (select ST_TileEnvelope(${z}, ${x}, ${y}) geom)
      select ST_XMin(tile.geom) xmin,
             ST_YMin(tile.geom) ymin,
             ST_XMax(tile.geom) xmax,
             ST_YMax(tile.geom) ymax
      from tile
      where tile.geom && ST_MakeEnvelope(-10683450.569, 3507542.354, -10583776.684, 3448227.220, 3857);`);

    if (rows.length === 0) {
        return res.status(204);
    }

    let xmin, ymin, xmax, ymax;
    for (const row of rows) {
        xmin = row.xmin;
        ymin = row.ymin;
        xmax = row.xmax;
        ymax = row.ymax;
    }

    const tileFilePath = `./tiles_reor/${z}-${x}-${y}.png`;
    if (!fs.existsSync(tileFilePath)) {
        const gdalwarp = spawn("gdalwarp", [
            "-srcnodata",
            0,
            //"-srcalpha",
            "-ts",
            512,
            512,
            "-te",
            xmin,
            ymin,
            xmax,
            ymax,
            REOR_COG_PATH,
            tileFilePath,
        ], { detached: true });
        gdalwarp.unref();
        
        gdalwarp.stdout.on('data', (data) => {
            //data started to be created
            //can add some logs
        });
        
        gdalwarp.stderr.on('data', (data) => {
            console.error(`stderr: ${data}`);
        });
        
        gdalwarp.on('close', (code) => {
            //spawn ended and closed
            fs.createReadStream(tileFilePath).pipe(res);
        });
    } else {
        fs.createReadStream(tileFilePath).pipe(res);
      }
});

module.exports = router;