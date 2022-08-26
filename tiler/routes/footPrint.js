var express = require('express');
var router = express.Router();
const { gzip } = require("node-gzip");
const config = require('../utils/config-helper').getConfig();
const pg = require('pg'),
    pool = new pg.Pool(config.pg);

router.get("/:z/:x/:y.mvt", async (req, res, next) => {
    const { z, x, y } = req.params;
    const client = await pool.connect();
    try {
        const { rows } = await client.query(`
            with mvtgeom as
            (
          select ST_AsMVTGeom(st_transform(st_boundary(mask_geom), 3857), ST_TileEnvelope(${z}, ${x}, ${y})) as geom
          from oam_meta
          where (is_cog_ready = true or is_cog_ready = false) and st_transform(mask_geom, 3857) && ST_TileEnvelope(${z}, ${x}, ${y})
            )
            select st_asmvt(mvtgeom.*) as mvt
            from mvtgeom;
          `);
        if (rows.length === 0) {
            return res.status(204);
        }
    } catch (err) {
        throw err;
    }
    finally {
        client.release();
    }
    const gz = await gzip(rows[0].mvt);

    res.writeHead(200, { "Content-Encoding": "gzip" });
    res.end(gz);
});

module.exports = router;