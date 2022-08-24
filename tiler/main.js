const express = require("express");
const morgan = require("morgan");
const { Client } = require("pg");
const spawn = require("spawndamnit");
const fs = require("fs");
const cors = require("cors");
const { Readable } = require("stream");
const { gzip, ungzip } = require("node-gzip");

//process.env.PGHOST = "localhost";
//process.env.PGUSER = "kalenik";
//process.env.PGPASSWORD = "kalenik";

const BASE_URL = "http://geocint.kontur.io/rastertiler";
const OAM_COGS_PATH = "/mnt/evo4tb/oam_cogs/";
const REOR_COG_PATH = "/home/gis/rastertiler/reor_colored_cog.tif";

const app = express();
const client = new Client();

app.use(
  morgan(":method :url :status :res[content-length] - :response-time ms")
);
app.use(cors());

app.get("/tilejson.json", async (req, res) => {
  res.json({
    tilejson: "2.2.0",
    version: "1.0.0",
    scheme: "xyz",
    tiles: [`${BASE_URL}/tiles/{z}/{x}/{y}.png`],
    minzoom: 4,
    maxzoom: 20,
    center: [27.580661773681644, 53.85617102825757, 13],
  });
});

// localhost:3000/tiles/14/9440/5270.png
app.get("/tiles/:z/:x/:y.png", async (req, res) => {
  const { z, x, y } = req.params;

  const { rows } = await client.query(`
    with tile as (select ST_TileEnvelope(${z}, ${x}, ${y}) geom)
    select ST_XMin(tile.geom) xmin,
           ST_YMin(tile.geom) ymin,
	   ST_XMax(tile.geom) xmax,
	   ST_YMax(tile.geom) ymax,
	   uuid
    from tile, oam_meta
    where is_rgb and tile.geom && ST_Transform(oam_meta.geom, 3857)
    order by resolution_in_meters desc nulls last, uploaded_at desc`);

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

  const tileFilePath = `./tiles/${z}-${x}-${y}.png`;
  if (!fs.existsSync(tileFilePath)) {
    const gdalwarp = spawn("/home/gis/rastertiler/gdal/build/apps/gdalwarp", [
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
    //gdalwarp.on("stdout", (data) => console.log(data.toString()));
    gdalwarp.on("stderr", (data) => {
      error = true;
      console.error(data.toString());
    });

    await gdalwarp;

    if (error) {
      console.error(">>>gdalwarp ERR");
      throw new Error();
    }
  }

  fs.createReadStream(tileFilePath).pipe(res);
});

app.get("/outlines/:z/:x/:y.mvt", async (req, res) => {
  const { z, x, y } = req.params;

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

  const gz = await gzip(rows[0].mvt);

  res.writeHead(200, { "Content-Encoding": "gzip" });
  res.end(gz);
});

app.get("/reor/tilejson.json", async (req, res) => {
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

app.get("/tiles/reor/:z/:x/:y.png", async (req, res) => {
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
    ]);

    gdalwarp.on("stdout", (data) => console.log(data.toString()));
    gdalwarp.on("stderr", (data) => console.error(data.toString()));

    await gdalwarp;
  }

  fs.createReadStream(tileFilePath).pipe(res);
});

const start = async () => {
  try {
    await client.connect();
    app.listen(7802);
  } catch (err) {
    console.error(err);
    process.exit(1);
  }
};

start();
