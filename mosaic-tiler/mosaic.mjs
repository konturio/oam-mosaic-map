import got from "got";
import sharp from "sharp";
import PQueue from "p-queue";
import cover from "@mapbox/tile-cover";
import * as db from "./db.mjs";
import { cacheGet, cachePut } from "./cache.mjs";

const TITILER_BASE_URL = process.env.TITILER_BASE_URL;
const TILE_SIZE = 512;

const tileRequestQueue = new PQueue({ concurrency: 32 });
const activeTileRequests = new Map();
const metadataRequestQueue = new PQueue({ concurrency: 32 });

function cacheGetTile(key, z, x, y, extension) {
  if (extension !== "png" && extension !== "jpg") {
    throw new Error(".png and .jpg are the only allowed extensions");
  }
  return cacheGet(`${key}/${z}/${x}/${y}.${extension}`);
}

function cachePutTile(tile, key, z, x, y, extension) {
  if (extension !== "png" && extension !== "jpg") {
    throw new Error(".png and .jpg are the only allowed extensions");
  }
  return cachePut(tile, `${key}/${z}/${x}/${y}.${extension}`);
}

class TileImage {
  constructor(buffer, extension) {
    this.buffer = buffer;
    this.extension = extension;
  }

  empty() {
    return this.buffer === null || this.buffer.length === 0;
  }
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
  try {
    const responsePromise = got(url, {
      throwHttpErrors: true,
    });

    const [response, buffer] = await Promise.all([
      responsePromise,
      responsePromise.buffer(),
    ]);

    if (response.statusCode === 204) {
      return null;
    }

    return buffer;
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

async function enqueueTileFetching(tileUrl, z, x, y) {
  const url = tileUrl.replace("{z}", z).replace("{x}", x).replace("{y}", y);
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
  if (buffer === null) {
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
    .toBuffer();
}

// TODO: ignore transparent tiles from input
// produces tile from 4 underlying children tiles
async function fromChildren(tiles) {
  const [upperLeft, upperRight, lowerLeft, lowerRight] = tiles;

  const composite = [];
  if (!upperLeft.empty()) {
    const downscaled = await downscaleTile(upperLeft.buffer);
    composite.push({ input: downscaled, top: 0, left: 0 });
  }
  if (!upperRight.empty()) {
    const downscaled = await downscaleTile(upperRight.buffer);
    composite.push({ input: downscaled, top: 0, left: TILE_SIZE / 2 });
  }
  if (!lowerLeft.empty()) {
    const downscaled = await downscaleTile(lowerLeft.buffer);
    composite.push({ input: downscaled, top: TILE_SIZE / 2, left: 0 });
  }
  if (!lowerRight.empty()) {
    const downscaled = await downscaleTile(lowerRight.buffer);
    composite.push({
      input: downscaled,
      top: TILE_SIZE / 2,
      left: TILE_SIZE / 2,
    });
  }

  const buffer = await sharp({
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

  return new TileImage(buffer, "png");
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
async function source(key, z, x, y, meta, geojson) {
  if (z > meta.maxzoom) {
    return new TileImage(null);
  }

  let tileBuffer = await cacheGetTile(key, z, x, y, "png");
  if (tileBuffer) {
    return new TileImage(tileBuffer, "png");
  }

  const tileCover = getTileCover(geojson, z);
  const intersects = tileCover.find((pos) => {
    return pos[0] === x && pos[1] === y && pos[2] === z;
  });
  if (!intersects) {
    await cachePutTile(null, key, z, x, y, "png");
    return new TileImage(null);
  }

  if (z >= meta.minzoom && z <= meta.maxzoom) {
    tileBuffer = await enqueueTileFetching(meta.tileUrl, z, x, y);
  } else if (z < meta.maxzoom) {
    const tiles = await Promise.all([
      source(key, z + 1, x * 2, y * 2, meta, geojson),
      source(key, z + 1, x * 2 + 1, y * 2, meta, geojson),
      source(key, z + 1, x * 2, y * 2 + 1, meta, geojson),
      source(key, z + 1, x * 2 + 1, y * 2 + 1, meta, geojson),
    ]);

    tileBuffer = (await fromChildren(tiles)).buffer;
  } else {
    return new TileImage(null);
  }

  await cachePutTile(tileBuffer, key, z, x, y, "png");

  return new TileImage(tileBuffer, "png");
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

  let tileBuffer = await cacheGetTile("__mosaic__", z, x, y, "png");
  if (tileBuffer) {
    return new TileImage(tileBuffer, "png");
  }

  tileBuffer = await cacheGetTile("__mosaic__", z, x, y, "jpg");
  if (tileBuffer) {
    return new TileImage(tileBuffer, "jpg");
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

  const tilePromises = [];
  if (z < 9) {
    for (const row of rows) {
      const meta = metadataByUuid[row.uuid];
      if (!meta) {
        continue;
      }

      if (meta.maxzoom < 9) {
        const key = keyFromS3Url(row.uuid);
        const geojson = JSON.parse(row.geojson);
        tilePromises.push(source(key, z, x, y, meta, geojson));
      }
    }

    tilePromises.push(
      fromChildren(
        await Promise.all([
          requestMosaic(z + 1, x * 2, y * 2),
          requestMosaic(z + 1, x * 2 + 1, y * 2),
          requestMosaic(z + 1, x * 2, y * 2 + 1),
          requestMosaic(z + 1, x * 2 + 1, y * 2 + 1),
        ])
      )
    );
  } else {
    for (const row of rows) {
      const meta = metadataByUuid[row.uuid];
      if (!meta) {
        continue;
      }

      const key = keyFromS3Url(row.uuid);
      const geojson = JSON.parse(row.geojson);
      tilePromises.push(source(key, z, x, y, meta, geojson));
    }
  }

  const tiles = await Promise.all(tilePromises);

  tileBuffer = await sharp({
    create: {
      width: TILE_SIZE,
      height: TILE_SIZE,
      channels: 4,
      background: { r: 0, g: 0, b: 0, alpha: 0 },
    },
  })
    .composite(
      tiles
        .filter((tile) => !tile.empty())
        .map((tile) => {
          return { input: tile.buffer, top: 0, left: 0 };
        })
    )
    .png()
    .toBuffer();

  let extension = "png";
  const tileImgStats = await sharp(tileBuffer).stats();
  if (tileImgStats.isOpaque) {
    extension = "jpg";
    tileBuffer = await sharp(tileBuffer).toFormat("jpeg").toBuffer();
  }

  await cachePutTile(tileBuffer, "__mosaic__", z, x, y, extension);

  return new TileImage(tileBuffer, extension);
}

export { requestMosaic, tileRequestQueue, metadataRequestQueue };
