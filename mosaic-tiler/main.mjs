import fs from "fs";
import sharp from "sharp";
import pg from "pg";
import got from "got";
import express from "express";
import morgan from "morgan";
import cors from "cors";
import node_gzip from "node-gzip";
import uniqueString from "unique-string";
import PQueue from "p-queue";

const { gzip } = node_gzip;

process.env.PGHOST = "localhost";
process.env.PGUSER = "gis";

const BASE_URL = "http://geocint.kontur.io/rastertiler";
const TILE_SIZE = 256;

const app = express();
const db = new pg.Client();

app.use(
  morgan(":method :url :status :res[content-length] - :response-time ms")
);
app.use(cors());

function wrapAsyncCallback(callback) {
  return (req, res, next) => {
    try {
      callback(req, res, next).catch(next);
    } catch (err) {
      console.log(">err", err);
      next(err);
    }
  };
}

app.get(
  "/tiles/tilejson.json",
  wrapAsyncCallback(async (req, res) => {
    res.json({
      tilejson: "2.2.0",
      version: "1.0.0",
      scheme: "xyz",
      tiles: [`${BASE_URL}/tiles/{z}/{x}/{y}.png`],
      minzoom: 5,
      maxzoom: 20,
      center: [27.580661773681644, 53.85617102825757, 13],
    });
  })
);

app.get(
  "/tiles/:z/:x/:y.png",
  wrapAsyncCallback(async (req, res) => {
    const { z, x, y } = req.params;

    const tile = await mosaic({ z: Number(z), x: Number(x), y: Number(y) });
    res.end(tile);
  })
);

app.get(
  "/outlines/:z/:x/:y.mvt",
  wrapAsyncCallback(async (req, res) => {
    const { z, x, y } = req.params;

    const { rows } = await db.query(`
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
  })
);

app.get(
  "/health",
  wrapAsyncCallback(async (req, res) => {
    res.status(200).send("Ok");
  })
);

app.use(function (err, req, res, next) {
  console.error(err.stack);
  res.status(500);
  res.end("Internal server error");
  next;
});

const tileRequestQueue = new PQueue({ concurrency: 3 });

async function downloadTile(uuid, { z, x, y }) {
  const tile = await tileRequestQueue.add(() =>
    got(`https://tiles.openaerialmap.org/${uuid}/${z}/${x}/${y}.png`, {
      throwHttpErrors: true,
      retry: { limit: 3 },
    }).buffer()
  );

  if (!tile.length) {
    return null;
  }

  const path = `./tiles/${uuid}/${z}/${x}/${y}.png`;
  const temp = `./tmp/download-${uniqueString()}`;
  await fs.promises.writeFile(temp, tile);
  await fs.promises.rename(temp, path);
  return path;
}

function pixelSizeAtZoom(z) {
  return ((20037508.342789244 / 512) * 2) / 2 ** z;
}

async function tile(uuid, { z, x, y }) {
  const { rows } = await db.query(
    `select true from oam_meta
     where ST_Transform(geom, 3857) && ST_TileEnvelope(${z}, ${x}, ${y})
      and ST_Area(ST_Transform(geom, 3857)) > ${pixelSizeAtZoom(z)}
      and uuid ~ '${uuid}';`
  );

  if (!rows.length || z > 25) {
    return "./bg.png";
  }

  const fsPath = `./tiles/${uuid}/${z}/${x}/${y}.png`;
  if (fs.existsSync(fsPath)) {
    return fsPath;
  }

  if (!fs.existsSync(`./tiles/${uuid}/${z}/${x}/`)) {
    fs.mkdirSync(`./tiles/${uuid}/${z}/${x}/`, { recursive: true });
  }

  const downloadedPath = await downloadTile(uuid, { z, x, y });
  if (downloadedPath) {
    return downloadedPath;
  }

  const children = [
    { z: z + 1, x: x * 2, y: y * 2 },
    { z: z + 1, x: x * 2 + 1, y: y * 2 },
    { z: z + 1, x: x * 2, y: y * 2 + 1 },
    { z: z + 1, x: x * 2 + 1, y: y * 2 + 1 },
  ];

  const tiles = await Promise.all(
    children.map(async (pos) => {
      const tilePath = await tile(uuid, pos);
      return sharp(tilePath)
        .resize({ width: TILE_SIZE / 2, height: TILE_SIZE / 2 })
        .toBuffer();
    })
  );

  const temp = `./tmp/render-${uniqueString()}`;
  const r = await sharp({
    create: {
      width: TILE_SIZE,
      height: TILE_SIZE,
      channels: 4,
      background: { r: 0, g: 0, b: 0, alpha: 0 },
    },
  })
    .composite([
      {
        input: tiles[0],
        top: 0,
        left: 0,
      },
      {
        input: tiles[1],
        top: 0,
        left: TILE_SIZE / 2,
      },
      {
        input: tiles[2],
        top: TILE_SIZE / 2,
        left: 0,
      },
      {
        input: tiles[3],
        top: TILE_SIZE / 2,
        left: TILE_SIZE / 2,
      },
    ])
    .png()
    .toFile(temp);

  await fs.promises.rename(temp, fsPath);

  return fsPath;
}

async function mosaic({ z, x, y }) {
  const { rows } = await db.query(
    `select uuid from oam_meta
     where ST_Transform(geom, 3857) && ST_TileEnvelope(${z}, ${x}, ${y})
     order by resolution_in_meters desc nulls last, uploaded_at desc;`
  );

  const inputs = [];
  for (const row of rows) {
    const uuid = row.uuid
      .replace("http://oin-hotosm.s3.amazonaws.com/", "")
      .replace("https://oin-hotosm.s3.amazonaws.com/", "")
      .replace(".tif", "");

    inputs.push(
      sharp(await tile(uuid, { z, x, y }))
        .png()
        .toBuffer()
    );
  }

  const tiles = await Promise.all(inputs);

  return sharp({
    create: {
      width: TILE_SIZE,
      height: TILE_SIZE,
      channels: 4,
      background: { r: 0, g: 0, b: 0, alpha: 0 },
    },
  })
    .composite(
      tiles.map((tile) => {
        return { input: tile, top: 0, left: 0 };
      })
    )
    .png()
    .toBuffer();
}

async function main() {
  try {
    await db.connect();
    app.listen(7802);
  } catch (err) {
    console.error(err);
    process.exit(1);
  }
}

main();
