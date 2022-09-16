import { promisify } from "util";
import fs from "fs";
import dotenv from "dotenv";
import sharp from "sharp";
import * as db from "./db.mjs";
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
const TMP_DIR_PATH = TILES_CACHE_DIR_PATH + "/tmp";

const gzip = promisify(zlib.gzip);

const app = express();

app.use(
  morgan(":method :url :status :res[content-length] - :response-time ms")
);
app.use(cors());

process.on("unhandledRejection", (error) => {
  console.log(">>>unhandledRejection", error.message);
  process.exit(1);
});

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
      minzoom: 1,
      maxzoom: 24,
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

let mvtConnection = null;
async function getMvtConnection() {
  if (!mvtConnection) {
    mvtConnection = await db.getClient();
  }
  return mvtConnection;
}

app.get(
  "/outlines/:z(\\d+)/:x(\\d+)/:y(\\d+).mvt",
  wrapAsyncCallback(async (req, res) => {
    const z = Number(req.params.z);
    const x = Number(req.params.x);
    const y = Number(req.params.y);
    if (isValidZxy(z, x, y)) {
      return res.status(404).send("Out of bounds");
    }

    // const dbClient = await db.getClient();
    const dbClient = await getMvtConnection();

    const { rows } = await dbClient.query({
      name: "mvt-outlines",
      text: `with oam_meta as (
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
      values: [z, x, y, z, x, y],
    });
    // .finally(() => dbClient.release());
    if (rows.length === 0) {
      return res.status(204).end();
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

// set priority equal to zoom level
const tileRequestQueue = new PQueue({ concurrency: 100 });
const activeTileRequests = new Map();

setInterval(() => {
  console.log(
    ">tile request queue size",
    tileRequestQueue.size,
    "should match",
    activeTileRequests.size
  );
  console.log(">image processing", sharp.counters());
  console.log(">db pool waiting count", db.getWaitingCount());
  // postgres query timeout
}, 1000);

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

  const buffer = tile || Buffer.from("");

  const path = `${TILES_CACHE_DIR_PATH}/${uuid}/${z}/${x}/${y}.png`;
  const temp = `${TMP_DIR_PATH}/${uniqueString()}`;
  await fs.promises.writeFile(temp, buffer);
  await fs.promises.rename(temp, path);
}

async function downloadTile(uuid, z, x, y) {
  const url = `https://tiles.openaerialmap.org/${uuid}/${z}/${x}/${y}.png`;
  if (activeTileRequests.get(url)) {
    return activeTileRequests.get(url);
  }

  const request = tileRequestQueue
    .add(
      async () => {
        const tile = await got(url, {
          throwHttpErrors: true,
          retry: { limit: 3 },
        }).buffer();

        if (!tile.length) {
          return null;
        }

        return tile;
      }
      // { priority: z }
    )
    .finally(() => {
      activeTileRequests.delete(url);
    });

  activeTileRequests.set(url, request);
  return request;
}

function pixelSizeAtZoom(z) {
  return ((20037508.342789244 / 512) * 2) / 2 ** z;
}

function downscale(tile) {
  return sharp(tile)
    .resize({ width: TILE_SIZE / 2, height: TILE_SIZE / 2 })
    .toBuffer();
}

async function fromChildren(tiles) {
  const upperLeft = tiles[0];
  const upperRight = tiles[1];
  const lowerLeft = tiles[2];
  const lowerRight = tiles[3];

  const composite = [];
  if (upperLeft && upperLeft.length) {
    const downscaled = await downscale(upperLeft);
    composite.push({ input: downscaled, top: 0, left: 0 });
  }
  if (upperRight && upperRight.length) {
    const downscaled = await downscale(upperRight);
    composite.push({ input: downscaled, top: 0, left: TILE_SIZE / 2 });
  }
  if (lowerLeft && lowerLeft.length) {
    const downscaled = await downscale(lowerLeft);
    composite.push({ input: downscaled, top: TILE_SIZE / 2, left: 0 });
  }
  if (lowerRight && lowerRight.length) {
    const downscaled = await downscale(lowerRight);
    composite.push({
      input: downscaled,
      top: TILE_SIZE / 2,
      left: TILE_SIZE / 2,
    });
  }

  return sharp({
    create: {
      width: TILE_SIZE,
      height: TILE_SIZE,
      channels: 4,
      background: { r: 0, g: 0, b: 0, alpha: 0 },
    },
  })
    .composite(composite)
    .png()
    .toBuffer();
}

async function source(uuid, z, x, y, db) {
  // if (z > 26) {
  // return res.status(204).end();
  // return null;
  // }

  let tile = await cacheGet(uuid, z, x, y);
  if (tile) {
    return tile;
  }

  // if (z < 14) {
  //   const { rows } = await db.query({
  //     name: "check-if-zxy-tile-has-image-with-uuid",
  //     text: `with oam_meta as (
  //       select
  //         properties->>'gsd' as resolution_in_meters,
  //         properties->>'uploaded_at' as uploaded_at,
  //         properties->>'uuid' as uuid,
  //         geom
  //       from public.layers_features
  //       where layer_id = (select id from public.layers where public_id = 'openaerialmap')
  //     )
  //     select true
  //     from oam_meta
  //     where ST_TileEnvelope($1, $2, $3) && ST_Transform(geom, 3857) and uuid ~ $4`,
  //     values: [z, x, y, uuid],
  //   });

  //   if (!rows.length) {
  //     await cachePut(null, uuid, z, x, y);
  //     return null;
  //   }
  // }
  // .finally(() => dbClient.release());

  tile = await downloadTile(uuid, z, x, y);

  if (!tile && z <= 25) {
    const tiles = await Promise.all([
      source(uuid, z + 1, x * 2, y * 2, db),
      source(uuid, z + 1, x * 2 + 1, y * 2, db),
      source(uuid, z + 1, x * 2, y * 2 + 1, db),
      source(uuid, z + 1, x * 2 + 1, y * 2 + 1, db),
    ]);

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

  if (z < 9) {
    const tiles = await Promise.all([
      mosaic(z + 1, x * 2, y * 2),
      mosaic(z + 1, x * 2 + 1, y * 2),
      mosaic(z + 1, x * 2, y * 2 + 1),
      mosaic(z + 1, x * 2 + 1, y * 2 + 1),
    ]);

    tile = await fromChildren(tiles);
  } else {
    let dbClient;
    let tiles;
    try {
      dbClient = await db.getClient();
      const { rows } = await dbClient
        .query({
          name: "get-image-uuid-in-zxy-tile",
          text: `with oam_meta as (
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
          values: [z, x, y],
        })
        .finally(() => dbClient.release());

      const sources = [];
      for (const row of rows) {
        const uuid = row.uuid
          .replace("http://oin-hotosm.s3.amazonaws.com/", "")
          .replace("https://oin-hotosm.s3.amazonaws.com/", "")
          .replace(".tif", "");

        sources.push(source(uuid, z, x, y, db));
      }

      tiles = await Promise.all(sources);
    } finally {
      // if (dbClient) {
      //   dbClient.release();
      // }
    }

    tile = await sharp({
      create: {
        width: TILE_SIZE,
        height: TILE_SIZE,
        channels: 4,
        background: { r: 0, g: 0, b: 0, alpha: 0 },
      },
    })
      .composite(
        tiles
          .filter((tile) => tile && tile.length)
          .map((tile) => {
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
  let dbClient;
  try {
    if (!fs.existsSync(TMP_DIR_PATH)) {
      fs.mkdirSync(TMP_DIR_PATH, { recursive: true });
    }

    // await db.pool.connect();
    // dbClient = await db.getClient();
    app.listen(PORT, () => {
      console.log(`mosaic-tiler server is listening on port ${PORT}`);
    });
  } catch (err) {
    console.error(err);
    process.exit(1);
  } finally {
    // dbClient.release();
  }
}

main();
