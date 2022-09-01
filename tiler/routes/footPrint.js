const express = require("express");
const router = express.Router();
const { gzip } = require("node-gzip");
const config = require("../utils/config-helper").getConfig();
const pg = require("pg"),
  pool = new pg.Pool(config.pg);

router.get("/:z/:x/:y.mvt", async (req, res, next) => {
  const { z, x, y } = req.params;
  let client;
  try {
    client = await pool.connect();
    const { rows } = await client.query(`
        with oam_meta as (
          select geom from public.layers_features
          where layer_id = (select id from public.layers where public_id = 'openaerialmap')
        ),
        mvtgeom as (
          select ST_AsMVTGeom(ST_Transform(ST_Boundary(geom), 3857), ST_TileEnvelope(${z}, ${x}, ${y})) as geom
          from oam_meta
          where st_transform(geom, 3857) && ST_TileEnvelope(${z}, ${x}, ${y})
        )
        select st_asmvt(mvtgeom.*) as mvt
        from mvtgeom;`);
    if (rows.length === 0) {
      return res.status(204);
    }

    const gz = await gzip(rows[0].mvt);

    res.writeHead(200, { "Content-Encoding": "gzip" });
    res.end(gz);
  } catch (err) {
    throw err;
  } finally {
    client.release();
  }
});

module.exports = router;
