const express = require("express");
const router = express.Router();
const config = require("../utils/config-helper").getConfig();
const fs = require("fs");
const spawn = require("spawndamnit");
const pg = require("pg"),
  pool = new pg.Pool(config.pg);
const OAM_COGS_PATH =  "/" + config.app.oam_path;
const BASE_URL = config.app.base_url;

router.get("/tilejson.json", async (req, res) => {
  try {
    res.json({
      tilejson: "2.2.0",
      version: "1.0.0",
      scheme: "xyz",
      tiles: [`${BASE_URL}/tiles/{z}/{x}/{y}.png`],
      minzoom: 4,
      maxzoom: 20,
      center: [27.580661773681644, 53.85617102825757, 13],
    });
  } catch (err) {
    throw err;
  }
});

// localhost:3000/tiles/14/9440/5270.png
router.get("/:z/:x/:y.png", async (req, res) => {
  let client;
  const { z, x, y } = req.params;
  try {
    client = await pool.connect();
    const { rows } =
      await client.query(`with oam_meta as (select properties->'gsd' as resolution_in_meters, 
        properties->'uploaded_at' as uploaded_at, 
        properties->'uuid' as uuid, 
        geom
      from public.layers_features
      where layer_id = (select id
        from public.layers
        where public_id = 'openaerialmap')
      ), tile as (select ST_TileEnvelope(${z}, ${x}, ${y}) geom)
      select ST_XMin(tile.geom) xmin,
          ST_YMin(tile.geom) ymin,
          ST_XMax(tile.geom) xmax,
          ST_YMax(tile.geom) ymax,
          uuid
      from tile, oam_meta
      where tile.geom && ST_Transform(oam_meta.geom, 3857)
      order by resolution_in_meters desc nulls last, uploaded_at desc nulls last`);

    let xmin, ymin, xmax, ymax;
    let uuids = [];
    for (const row of rows) {
      xmin = row.xmin;
      ymin = row.ymin;
      xmax = row.xmax;
      ymax = row.ymax;
      const path = OAM_COGS_PATH + row.uuid.split("/").pop();
      if (fs.existsSync(path)) uuids.push(path);
    }

    if (uuids.length === 0) {
      return res.status(204);
    }

    const tileFilePath = `/usr/src/app/tiles-cache/${z}-${x}-${y}.png`;
    if (!fs.existsSync(tileFilePath)) {
      const gdalwarp = spawn("gdalwarp", [
        //"-srcnodata",
        //0,
        "-t_srs",
        "epsg:3857",
        "-dstalpha",
        "-ts",
        512,
        512,
        "-te",
        xmin,
        ymin,
        xmax,
        ymax,
        ...uuids,
        tileFilePath,
      ]);

      let error = false;
      gdalwarp.on("stdout", (data) => console.log(data.toString()));
      gdalwarp.on("stderr", (data) => {
        error = true;
        console.error(data.toString());
      });

      await gdalwarp;

      if (error) {
        throw new Error();
      }
    }

    fs.createReadStream(tileFilePath).pipe(res);
  } catch (err) {
    throw err;
  } finally {
    client.release();
  }
});

module.exports = router;
