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
import cover from "@mapbox/tile-cover";
import { dirname } from "path";

dotenv.config({ path: ".env" });

const PORT = process.env.PORT;
const BASE_URL = process.env.BASE_URL;
const TITILER_BASE_URL = process.env.TITILER_BASE_URL;
const TILE_SIZE = 512;
const TILES_CACHE_DIR_PATH = process.env.TILES_CACHE_DIR_PATH;
const TMP_DIR_PATH = TILES_CACHE_DIR_PATH + "/tmp";

const gzip = promisify(zlib.gzip);

const app = express();

app.use(
  morgan(":method :url :status :res[content-length] - :response-time ms")
);
app.use(cors());

process.on("unhandledRejection", (error) => {
  console.log(">>>unhandledRejection", error);
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
      minzoom: 0,
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

    const tile = await requestMosaic(z, x, y);
    res.type("png");
    res.end(tile);
  })
);

// separate connection for mvt outlines debug endpoint to make it respond when
// all other connection in pool are busy
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

app.post("/purge_mosaic_cache", (req, res) => {
  fs.rmSync(`${TILES_CACHE_DIR_PATH}/__mosaic__`);
  res.end("Ok");
});

app.use(function (err, req, res, next) {
  console.error(err.stack);
  res.status(500);
  res.end("Internal server error");
  next;
});

const tileRequestQueue = new PQueue({ concurrency: 32 });
const activeTileRequests = new Map();

const metadataRequestQueue = new PQueue({ concurrency: 32 });

setInterval(() => {
  console.log(">tile request queue size", tileRequestQueue.size);
  console.log(">metadata request queue size", metadataRequestQueue.size);
  console.log(">image processing", sharp.counters());
  console.log(">db pool waiting count", db.getWaitingCount());
}, 1000);

async function cacheGet(cacheKey) {
  try {
    return await fs.promises.readFile(`${TILES_CACHE_DIR_PATH}/${cacheKey}`);
  } catch (err) {
    if (err.code === "ENOENT") {
      return null;
    }

    throw err;
  }
}

async function cachePut(buffer, key) {
  const path = `${TILES_CACHE_DIR_PATH}/${key}`;
  if (!fs.existsSync(dirname(path))) {
    fs.mkdirSync(dirname(path), {
      recursive: true,
    });
  }

  // create empty file if buffer param is falsy value
  buffer = buffer || Buffer.from("");

  // write into temp file and then rename to actual name to avoid read of inflight tiles from concurrent requests
  const temp = `${TMP_DIR_PATH}/${uniqueString()}`;
  await fs.promises.writeFile(temp, buffer);
  await fs.promises.rename(temp, path);
}

function cacheGetTile(key, z, x, y) {
  return cacheGet(`${key}/${z}/${x}/${y}.png`);
}

function cachePutTile(tile, key, z, x, y) {
  return cachePut(tile, `${key}/${z}/${x}/${y}.png`);
}

function keyFromS3Url(url) {
  return url
    .replace("http://oin-hotosm.s3.amazonaws.com/", "")
    .replace("https://oin-hotosm.s3.amazonaws.com/", "")
    .replace("http://oin-hotosm-staging.s3.amazonaws.com/", "")
    .replace("https://oin-hotosm-staging.s3.amazonaws.com/", "")
    .replace(".tif", "");
}

async function fetchTile(url) {
  const responsePromise = got(url, {
    throwHttpErrors: false,
  });

  const [response, buffer] = await Promise.all([
    responsePromise,
    responsePromise.buffer(),
  ]);

  return { response, buffer };
}

async function enqueueTileFetching(uuid, z, x, y) {
  const url = uuid.replace("{z}", z).replace("{x}", x).replace("{y}", y);
  if (activeTileRequests.get(url)) {
    return activeTileRequests.get(url);
  }

  const request = tileRequestQueue
    .add(() => fetchTile(url))
    .finally(() => {
      activeTileRequests.delete(url);
    });

  activeTileRequests.set(url, request);
  return request;
}

async function cacheGetMetadata(key) {
  const buffer = await cacheGet(`__metadata__/${key}.json`);
  if (buffer === null || !buffer.length) {
    return null;
  }

  return JSON.parse(buffer.toString());
}

function cachePutMetadata(metadata, key) {
  const buffer = Buffer.from(JSON.stringify(metadata));
  return cachePut(buffer, `__metadata__/${key}.json`);
}

async function getGeotiffMetadata(uuid) {
  const key = keyFromS3Url(uuid);

  let metadata = await cacheGetMetadata(key);
  if (metadata === null) {
    metadata = await enqueueMetadataFetching(uuid);
  }

  await cachePutMetadata(metadata, key);

  if (!metadata) {
    return null;
  }

  const tileUrl = new URL(
    `${TITILER_BASE_URL}/cog/tiles/WebMercatorQuad/___z___/___x___/___y___@2x`
  );
  tileUrl.searchParams.append("url", uuid);
  for (let i = 0; i < metadata.band_metadata.length; ++i) {
    if (metadata.colorinterp[i] != "undefined") {
      const [idx] = metadata.band_metadata[i];
      tileUrl.searchParams.append("bidx", idx);
    }
  }

  return {
    minzoom: metadata.minzoom,
    maxzoom: metadata.maxzoom,
    tileUrl: tileUrl.href
      .replace("___z___", "{z}")
      .replace("___x___", "{x}")
      .replace("___y___", "{y}"),
  };
}

async function fetchTileMetadata(uuid) {
  try {
    const url = new URL(`${TITILER_BASE_URL}/cog/info`);
    url.searchParams.append("url", uuid);
    const metadata = await got(url.href).json();
    return metadata;
  } catch (err) {
    if (
      err.response &&
      (err.response.statusCode === 404 || err.response.statusCode === 500)
    ) {
      return null;
    } else {
      throw err;
    }
  }
}

const activeMetaRequests = new Map();
// deduplicates and limits number of concurrent calls for fetchTileMetadata function
function enqueueMetadataFetching(uuid) {
  if (activeMetaRequests.get(uuid)) {
    return activeMetaRequests.get(uuid);
  }

  const request = metadataRequestQueue
    .add(() => fetchTileMetadata(uuid))
    .finally(() => {
      activeMetaRequests.delete(uuid);
    });

  activeMetaRequests.set(uuid, request);

  return request;
}

function downscaleTile(buffer) {
  return sharp(buffer)
    .resize({ width: TILE_SIZE / 2, height: TILE_SIZE / 2 })
    .png()
    .toBuffer();
}

// TODO: ignore transparent tiles from input
// produces tile from 4 underlying children tiles
async function fromChildren(tiles) {
  const [upperLeft, upperRight, lowerLeft, lowerRight] = tiles;

  const composite = [];
  if (upperLeft && upperLeft.length) {
    const downscaled = await downscaleTile(upperLeft);
    composite.push({ input: downscaled, top: 0, left: 0 });
  }
  if (upperRight && upperRight.length) {
    const downscaled = await downscaleTile(upperRight);
    composite.push({ input: downscaled, top: 0, left: TILE_SIZE / 2 });
  }
  if (lowerLeft && lowerLeft.length) {
    const downscaled = await downscaleTile(lowerLeft);
    composite.push({ input: downscaled, top: TILE_SIZE / 2, left: 0 });
  }
  if (lowerRight && lowerRight.length) {
    const downscaled = await downscaleTile(lowerRight);
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

const tileCoverCache = {
  0: new WeakMap(),
  1: new WeakMap(),
  2: new WeakMap(),
  3: new WeakMap(),
  4: new WeakMap(),
  5: new WeakMap(),
  6: new WeakMap(),
  7: new WeakMap(),
  8: new WeakMap(),
  9: new WeakMap(),
  10: new WeakMap(),
  11: new WeakMap(),
  12: new WeakMap(),
  13: new WeakMap(),
  14: new WeakMap(),
  15: new WeakMap(),
  16: new WeakMap(),
  17: new WeakMap(),
  18: new WeakMap(),
  19: new WeakMap(),
  20: new WeakMap(),
  21: new WeakMap(),
  22: new WeakMap(),
  23: new WeakMap(),
  24: new WeakMap(),
  25: new WeakMap(),
};

function getTileCover(geojson, zoom) {
  if (tileCoverCache[zoom].get(geojson)) {
    return tileCoverCache[zoom].get(geojson);
  }

  const tileCover = cover.tiles(geojson, { min_zoom: zoom, max_zoom: zoom });
  tileCoverCache[zoom].set(geojson, tileCover);

  return tileCover;
}

// request tile for single image
// uuid -- s3 image url
// z, x, y -- coordinates
// meta -- object that contains minzoom, maxzoom and tile url template
// geojson -- image outline
async function source(uuid, z, x, y, meta, geojson) {
  if (z > meta.maxzoom) {
    return null;
  }

  let tile = await cacheGetTile(uuid, z, x, y);
  if (tile) {
    return tile;
  }

  const tileCover = getTileCover(geojson, z);
  const intersects = tileCover.find((tile) => {
    return tile[0] === x && tile[1] === y && tile[2] === z;
  });
  if (!intersects) {
    await cachePutTile(null, uuid, z, x, y);
    return null;
  }

  if (z >= meta.minzoom && z <= meta.maxzoom) {
    const { response, buffer } = await enqueueTileFetching(
      meta.tileUrl,
      z,
      x,
      y
    );

    if (response.statusCode === 204 || response.statusCode === 404) {
      // oam tiler returns status 204 for empty tiles and titiler returns 404
      tile = null;
    } else if (response.statusCode === 200) {
      tile = buffer;
    } else if (response.statusCode === 500) {
      // if dynamic tiler (titiler) failed to produce tile -- don't display it
      console.log(">>>tile request failed with status 500", response);
      tile = null;
    } else {
      throw new Error(
        `>>>tile request failed with status = ${response.statusCode} uuid = ${uuid} ${z}/${x}/${y}`
      );
    }
  } else if (z < meta.maxzoom) {
    const tiles = await Promise.all([
      source(uuid, z + 1, x * 2, y * 2, meta, geojson),
      source(uuid, z + 1, x * 2 + 1, y * 2, meta, geojson),
      source(uuid, z + 1, x * 2, y * 2 + 1, meta, geojson),
      source(uuid, z + 1, x * 2 + 1, y * 2 + 1, meta, geojson),
    ]);

    tile = await fromChildren(tiles);
  } else {
    return null;
  }

  await cachePutTile(tile, uuid, z, x, y);

  return tile;
}

const activeMosaicRequests = new Map();
// wrapper that deduplicates mosiac function calls
function requestMosaic(z, x, y) {
  const key = JSON.stringify([z, x, y]);
  if (activeMosaicRequests.has(key)) {
    return activeMosaicRequests.get(key);
  }

  const request = mosaic(z, x, y).finally(() =>
    activeMosaicRequests.delete(key)
  );
  activeMosaicRequests.set(key, request);

  return request;
}

// request tile for mosaic
async function mosaic(z, x, y) {
  let dbClient;
  let rows;

  let tile = await cacheGetTile("__mosaic__", z, x, y);
  if (tile) {
    return tile;
  }

  try {
    dbClient = await db.getClient();
    const dbResponse = await dbClient.query({
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
      select uuid, ST_AsGeoJSON(ST_Envelope(geom)) geojson
      from oam_meta
      where ST_TileEnvelope($1, $2, $3) && ST_Transform(geom, 3857)
      order by resolution_in_meters desc nulls last, uploaded_at desc nulls last`,
      values: [z, x, y],
    });
    rows = dbResponse.rows;
  } catch (err) {
    throw err;
  } finally {
    if (dbClient) {
      dbClient.release();
    }
  }

  const metadataByUuid = {};
  await Promise.all(
    rows.map((row) => {
      const f = async () => {
        metadataByUuid[row.uuid] = await getGeotiffMetadata(row.uuid);
      };

      return f();
    })
  );

  if (z < 9) {
    const sources = [];
    for (const row of rows) {
      const meta = metadataByUuid[row.uuid];
      if (!meta) {
        continue;
      }

      if (meta.maxzoom < 9) {
        const key = keyFromS3Url(row.uuid);
        const geojson = JSON.parse(row.geojson);
        sources.push(source(key, z, x, y, meta, geojson));
      }
    }

    sources.push(
      fromChildren(
        await Promise.all([
          requestMosaic(z + 1, x * 2, y * 2),
          requestMosaic(z + 1, x * 2 + 1, y * 2),
          requestMosaic(z + 1, x * 2, y * 2 + 1),
          requestMosaic(z + 1, x * 2 + 1, y * 2 + 1),
        ])
      )
    );

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
        tiles
          .filter((tile) => tile && tile.length)
          .map((tile) => {
            return { input: tile, top: 0, left: 0 };
          })
      )
      .png()
      .toBuffer();
  } else {
    let tiles;

    const sources = [];
    for (const row of rows) {
      const f = async () => {
        const key = keyFromS3Url(row.uuid);

        // const meta = await getGeotiffMetadata(row.uuid);
        const meta = metadataByUuid[row.uuid];
        if (!meta) {
          return null;
        }

        const geojson = JSON.parse(row.geojson);
        const tile = await source(key, z, x, y, meta, geojson);

        return tile;
      };

      sources.push(f());
    }

    tiles = await Promise.all(sources);

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

  await cachePutTile(tile, "__mosaic__", z, x, y);

  return tile;
}

async function main() {
  try {
    if (!fs.existsSync(TMP_DIR_PATH)) {
      fs.mkdirSync(TMP_DIR_PATH, { recursive: true });
    }

    app.listen(PORT, () => {
      console.log(`mosaic-tiler server is listening on port ${PORT}`);
    });
  } catch (err) {
    console.error(err);
    process.exit(1);
  }
}

main();
