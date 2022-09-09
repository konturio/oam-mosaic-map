import { promisify } from "util";
import fs from "fs";
import dotenv from "dotenv";
import sharp from "sharp";
import pg from "pg";
import got from "got";
import express from "express";
import morgan from "morgan";
import cors from "cors";
import zlib from "zlib";
import uniqueString from "unique-string";
import PQueue from "p-queue";

dotenv.config({ path: ".env" });

const PORT = process.env.PORT;
const BASE_URL = process.env.BASE_URL;
const TILE_SIZE = 256;
const TILES_CACHE_DIR_PATH = process.env.TILES_CACHE_DIR_PATH;
const TMP_DIR_PATH = process.env.TMP_DIR_PATH || "/tmp";

const gzip = promisify(zlib.gzip);

const app = express();
const db = new pg.Client({
  ssl: {
    rejectUnauthorized: false,
  },
});

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

function isValidZxy(z, x, y) {
  return z < 0 || x < 0 || y < 0 || x >= Math.pow(2, z) || y >= Math.pow(2, z);
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
  "/tiles/:z(\\d+)/:x(\\d+)/:y(\\d+).png",
  wrapAsyncCallback(async (req, res) => {
    const z = Number(req.params.z);
    const x = Number(req.params.x);
    const y = Number(req.params.y);
    if (isValidZxy(z, x, y)) {
      return res.status(404).send("Out of bounds");
    }

    const tile = await mosaic(z, x, y);
    res.end(tile);
  })
);

app.get(
  "/outlines/:z(\\d+)/:x(\\d+)/:y(\\d+).mvt",
  wrapAsyncCallback(async (req, res) => {
    const z = Number(req.params.z);
    const x = Number(req.params.x);
    const y = Number(req.params.y);
    if (isValidZxy(z, x, y)) {
      return res.status(404).send("Out of bounds");
    }

    const { rows } = await db.query(
      `with oam_meta as (
         select geom from public.layers_features
         where layer_id = (select id from public.layers where public_id = 'openaerialmap')
       ),
       mvtgeom as (
         select
           ST_AsMVTGeom(
             ST_Transform(ST_Boundary(geom), 3857),
             ST_TileEnvelope($1, $2, $3)
           ) geom
         from oam_meta
         where ST_Transform(geom, 3857) && ST_TileEnvelope($4, $5, $6)
      )
      select ST_AsMVT(mvtgeom.*) as mvt
      from mvtgeom`,
      [z, x, y, z, x, y]
    );
    if (rows.length === 0) {
      return res.status(204);
    }

    res.writeHead(200, { "Content-Encoding": "gzip" });
    res.end(await gzip(rows[0].mvt));
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

async function cacheGet(uuid, z, x, y) {
  try {
    return await fs.promises.readFile(
      `${TILES_CACHE_DIR_PATH}/${uuid}/${z}/${x}/${y}.png`
    );
  } catch (err) {
    if (err.code === "ENOENT") {
      return null;
    }

    throw err;
  }
}

async function cachePut(tile, uuid, z, x, y) {
  if (!fs.existsSync(`${TILES_CACHE_DIR_PATH}/${uuid}/${z}/${x}/`)) {
    fs.mkdirSync(`${TILES_CACHE_DIR_PATH}/${uuid}/${z}/${x}/`, {
      recursive: true,
    });
  }

  const path = `${TILES_CACHE_DIR_PATH}/${uuid}/${z}/${x}/${y}.png`;
  const temp = `${TMP_DIR_PATH}/${uniqueString()}`;
  await fs.promises.writeFile(temp, tile);
  await fs.promises.rename(temp, path);
}

async function downloadTile(uuid, z, x, y) {
  const tile = await tileRequestQueue.add(() =>
    got(`https://tiles.openaerialmap.org/${uuid}/${z}/${x}/${y}.png`, {
      throwHttpErrors: true,
      retry: { limit: 3 },
    }).buffer()
  );

  if (!tile.length) {
    return null;
  }

  return tile;
}

function pixelSizeAtZoom(z) {
  return ((20037508.342789244 / 512) * 2) / 2 ** z;
}

function fromChildren(tiles) {
  return sharp({
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
    .toBuffer();
}

async function source(uuid, z, x, y) {
  const { rows } = await db.query(
    `with oam_meta as (
        select
          properties->>'gsd' as resolution_in_meters, 
          properties->>'uploaded_at' as uploaded_at, 
          properties->>'uuid' as uuid, 
          geom
        from public.layers_features
        where layer_id = (select id from public.layers where public_id = 'openaerialmap')
      )
      select true
      from oam_meta
      where ST_TileEnvelope($1, $2, $3) && ST_Transform(geom, 3857)
        and ST_Area(ST_Transform(geom, 3857)) > $4
        and uuid ~ $5
      order by resolution_in_meters desc nulls last, uploaded_at desc nulls last`,
    [z, x, y, pixelSizeAtZoom(z), uuid]
  );
  if (!rows.length) {
    return null;
  }

  let tile = await cacheGet(uuid, z, x, y);
  if (tile) {
    return tile;
  }

  tile = await downloadTile(uuid, z, x, y);
  if (!tile) {
    const children = [
      { z: z + 1, x: x * 2, y: y * 2 },
      { z: z + 1, x: x * 2 + 1, y: y * 2 },
      { z: z + 1, x: x * 2, y: y * 2 + 1 },
      { z: z + 1, x: x * 2 + 1, y: y * 2 + 1 },
    ];

    const tiles = await Promise.all(
      children.map(async ({ z, x, y }) => {
        const tile = await mosaic(z, x, y);
        return sharp(tile)
          .resize({ width: TILE_SIZE / 2, height: TILE_SIZE / 2 })
          .toBuffer();
      })
    );

    tile = await fromChildren(tiles);
  }

  await cachePut(tile, uuid, z, x, y);

  return tile;
}

async function mosaic(z, x, y) {
  let tile = await cacheGet("__mosaic__", z, x, y);
  if (tile) {
    return tile;
  }

  if (z < 11) {
    const children = [
      { z: z + 1, x: x * 2, y: y * 2 },
      { z: z + 1, x: x * 2 + 1, y: y * 2 },
      { z: z + 1, x: x * 2, y: y * 2 + 1 },
      { z: z + 1, x: x * 2 + 1, y: y * 2 + 1 },
    ];

    const tiles = await Promise.all(
      children.map(async ({ z, x, y }) => {
        const tile = await mosaic(z, x, y);
        return sharp(tile)
          .resize({ width: TILE_SIZE / 2, height: TILE_SIZE / 2 })
          .toBuffer();
      })
    );

    tile = await fromChildren(tiles);
  } else {
    const { rows } = await db.query(
      `with oam_meta as (
          select
              properties->>'gsd' as resolution_in_meters, 
              properties->>'uploaded_at' as uploaded_at, 
              properties->>'uuid' as uuid, 
              geom
          from public.layers_features
          where layer_id = (select id from public.layers where public_id = 'openaerialmap')
        )
        select uuid
        from oam_meta
        where ST_TileEnvelope($1, $2, $3) && ST_Transform(geom, 3857)
        order by resolution_in_meters desc nulls last, uploaded_at desc nulls last`,
      [z, x, y]
    );

    const sources = [];
    for (const row of rows) {
      const uuid = row.uuid
        .replace("http://oin-hotosm.s3.amazonaws.com/", "")
        .replace("https://oin-hotosm.s3.amazonaws.com/", "")
        .replace(".tif", "");

      sources.push(source(uuid, z, x, y));
    }

    const tiles = await Promise.all(sources);

    tile = await sharp({
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

  await cachePut(tile, "__mosaic__", z, x, y);

  return tile;
}

async function main() {
  try {
    await db.connect();
    app.listen(PORT, () => {
      console.log(`mosaic-tiler server is listening on port ${PORT}`);
    });
  } catch (err) {
    console.error(err);
    process.exit(1);
  }
}

main();
