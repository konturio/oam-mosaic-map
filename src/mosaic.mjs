import sharp from "sharp";
import * as db from "./db.mjs";
import { cacheGet, cachePut, cacheDelete, cachePurgeMosaic } from "./cache.mjs";
import { Tile, TileImage, constructParentTileFromChildren } from "./tile.mjs";
import { enqueueTileFetching } from "./titiler_fetcher.mjs";
import { getGeotiffMetadata } from "./metadata.mjs";
import { getTileCover } from "./tile_cover.mjs";
import { keyFromS3Url } from "./key_from_s3_url.mjs";
import { buildParametrizedFiltersQuery } from "./filters.mjs";

const OAM_LAYER_ID = process.env.OAM_LAYER_ID || "openaerialmap";

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

// request tile for single image
// uuid -- s3 image url
// z, x, y -- coordinates
// meta -- object that contains minzoom, maxzoom and tile url template
// geojson -- image outline
async function source(key, z, x, y, meta, geojson) {
  if (z > meta.maxzoom) {
    return Tile.createEmpty(z, x, y);
  }

  let tileBuffer = await cacheGetTile(key, z, x, y, "png");
  if (tileBuffer) {
    return new Tile(new TileImage(tileBuffer, "png"), z, x, y);
  }

  const tileCover = getTileCover(geojson, z);
  const intersects = tileCover.find((pos) => {
    return pos[0] === x && pos[1] === y && pos[2] === z;
  });
  if (!intersects) {
    await cachePutTile(null, key, z, x, y, "png");
    return Tile.createEmpty(z, x, y);
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

    const tile = await constructParentTileFromChildren(tiles, z, x, y);
    tileBuffer = tile.image.buffer;
  } else {
    return Tile.createEmpty(z, x, y);
  }

  await cachePutTile(tileBuffer, key, z, x, y, "png");

  return new Tile(new TileImage(tileBuffer, "png"), z, x, y);
}

function requestCachedMosaic256px(z, x, y) {
  return cachedMosaic256px(z, x, y);
}

const activeMosaicRequests = new Map();
function requestCachedMosaic512px(z, x, y) {
  // wrapper that deduplicates mosaic function calls
  const key = JSON.stringify([z, x, y]);
  if (activeMosaicRequests.has(key)) {
    return activeMosaicRequests.get(key);
  }

  const request = cachedMosaic512px(z, x, y).finally(() => activeMosaicRequests.delete(key));
  activeMosaicRequests.set(key, request);

  return request;
}

async function bufferToPixels(buffer) {

  if (!buffer || buffer.length === 0 || buffer === undefined) {
    console.log('Input buffer is empty or undefined', !buffer, buffer.length);
    return null;
  }

  const { data, info } = await sharp(buffer)
    .raw()
    .toBuffer({ resolveWithObject: true });

  const pixels = [];
  for (let i = 0; i < data.length; i += 4) {
    pixels.push({
      r: data[i],
      g: data[i + 1],
      b: data[i + 2],
      a: data[i + 3]
    });
  }

  return { pixels, width: info.width, height: info.height };
}

async function pixelsToBuffer(pixels, width, height) {
  if (!pixels || pixels.length === 0 || pixels === undefined) {
    console.log('Input pixels are empty or undefined');
    return null;
  }

  const data = new Uint8Array(width * height * 4); // Keeping with RGBA format from the PNG
  for (let i = 0; i < pixels.length; i++) {
    const blackThreshold = 33;
    if (pixels[i].r <= blackThreshold && pixels[i].g <= blackThreshold && pixels[i].b <= blackThreshold) {
      data[i * 4 + 3] = 0;
    }
  }

  // Use sharp to create a PNG buffer
  const buffer = await sharp(data, { raw: { width, height, channels: 4 } })
    .png()
    .toBuffer();

  return buffer;
}

async function cachedMosaic256px(z, x, y) {
  let tileBuffer = await cacheGetTile("__mosaic256__", z, x, y, "png");

  if (tileBuffer) {

    const pixels = await bufferToPixels(tileBuffer);
    const pixelsBuffer = pixelsToBuffer(pixels.pixels, pixels.width, pixels.height);

    return new Tile(new TileImage(pixelsBuffer, "png"), z, x, y);
  }

  tileBuffer = await cacheGetTile("__mosaic256__", z, x, y, "jpg");
  if (tileBuffer) {

    return new Tile(new TileImage(tileBuffer, "jpg"), z, x, y);
  }

  const mosaicTile = await mosaic256px(z, x, y);
  await cachePutTile(mosaicTile.image.buffer, "__mosaic256__", z, x, y, mosaicTile.image.extension);

  return mosaicTile;
}

async function cachedMosaic512px(z, x, y) {

  let tileBuffer = await cacheGetTile("__mosaic__", z, x, y, "png");

  if (tileBuffer) {

    const pixels = await bufferToPixels(tileBuffer);
    const pixelsBuffer = pixelsToBuffer(pixels.pixels, pixels.width, pixels.height);

    return new Tile(new TileImage(pixelsBuffer, "png"), z, x, y);
  }

  tileBuffer = await cacheGetTile("__mosaic__", z, x, y, "jpg");
  if (tileBuffer) {

    return new Tile(new TileImage(tileBuffer, "jpg"), z, x, y);
  }

  const mosaicTile = await mosaic512px(z, x, y);
  await cachePutTile(mosaicTile.image.buffer, "__mosaic__", z, x, y, mosaicTile.image.extension);

  return mosaicTile;
}

async function mosaic256px(z, x, y, filters = {}) {
  const request512pxFn = Object.keys(filters).length > 0 ? mosaic512px : requestCachedMosaic512px;
  let tile256;
  if (z % 2 === 0) {
    const tile512 = await request512pxFn(z, x, y, filters);
    tile256 = await tile512.scale(0.5);
  } else {
    const parent512 = await request512pxFn(z - 1, x >> 1, y >> 1, filters);
    tile256 = await parent512.extractChild(z, x, y);
  }

  tile256.image.transformInJpegIfFullyOpaque();

  return tile256;
}

async function mosaic512px(z, x, y, filters = {}) {
  const request512pxFn = Object.keys(filters).length > 0 ? mosaic512px : requestCachedMosaic512px;

  let dbClient;
  let rows;

  const { sqlQuery, sqlQueryParams, queryTag } = buildParametrizedFiltersQuery(
    OAM_LAYER_ID,
    z,
    x,
    y,
    filters
  );

  try {
    dbClient = await db.getClient();
    const dbResponse = await dbClient.query({
      name: "get-image-uuid-in-zxy-tile" + queryTag,
      text: sqlQuery,
      values: sqlQueryParams,
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
    rows.map(async (row) => {
      metadataByUuid[row.uuid] = await getGeotiffMetadata(row.uuid);
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
      constructParentTileFromChildren(
        await Promise.all([
          request512pxFn(z + 1, x * 2, y * 2, filters),
          request512pxFn(z + 1, x * 2 + 1, y * 2, filters),
          request512pxFn(z + 1, x * 2, y * 2 + 1, filters),
          request512pxFn(z + 1, x * 2 + 1, y * 2 + 1, filters),
        ]),
        z,
        x,
        y
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

  const tileBuffer = await sharp({
    create: {
      width: 512,
      height: 512,
      channels: 4,
      background: { r: 0, g: 0, b: 0, alpha: 0 },
    },
  })
    .composite(
      tiles
        .filter((tile) => !tile.empty())
        .map((tile) => {
          return { input: tile.image.buffer, top: 0, left: 0 };
        })
    )
    .png()
    .toBuffer();

  const tile = new Tile(new TileImage(tileBuffer, "png"), z, x, y);
  await tile.image.transformInJpegIfFullyOpaque();

  return tile;
}

export { mosaic256px, requestCachedMosaic512px, requestCachedMosaic256px };
