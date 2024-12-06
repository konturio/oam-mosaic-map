import sharp from "sharp";
import * as db from "./db.mjs";
import { cacheGet, cachePut } from "./cache.mjs";
import { Tile, TileImage, constructParentTileFromChildren } from "./tile.mjs";
import { enqueueTileFetching } from "./titiler_fetcher.mjs";
import { getGeotiffMetadata } from "./metadata.mjs";
import { getTileCover } from "./tile_cover.mjs";
import { keyFromS3Url } from "./key_from_s3_url.mjs";
import { buildParametrizedFiltersQuery } from "./filters.mjs";
import { blendTiles } from "./tileBlender.mjs";

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

/**
 * Fetches a single source tile from a given image (identified by its S3 key and metadata).
 * - If zoom level exceeds maxzoom, returns an empty tile.
 * - Checks the cache first; if not found, fetches the tile or reconstructs it from sub-tiles.
 */
async function source(key, z, x, y, meta, geojson) {
  if (z > meta.maxzoom) {
    return Tile.createEmpty(z, x, y);
  }

  let tileBuffer = await cacheGetTile(key, z, x, y, "png");
  if (tileBuffer) {
    return new Tile(new TileImage(tileBuffer, "png"), z, x, y);
  }

  const tileCover = getTileCover(geojson, z);
  const intersects = tileCover.find((pos) => pos[0] === x && pos[1] === y && pos[2] === z);
  if (!intersects) {
    // If the tile doesn't intersect with image footprint, cache null and return empty
    await cachePutTile(null, key, z, x, y, "png");
    return Tile.createEmpty(z, x, y);
  }

  if (z >= meta.minzoom && z <= meta.maxzoom) {
    // Fetch tile directly from source if zoom is within the image range
    tileBuffer = await enqueueTileFetching(meta.tileUrl, z, x, y);
  } else if (z < meta.maxzoom) {
    // If zoom is less than maxzoom but less detailed than needed, build tile from children
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

// This map ensures that concurrent requests for the same mosaic512px tile are deduplicated
const activeMosaicRequests = new Map();
function requestCachedMosaic512px(z, x, y) {
  const key = JSON.stringify([z, x, y]);
  if (activeMosaicRequests.has(key)) {
    return activeMosaicRequests.get(key);
  }

  const request = cachedMosaic512px(z, x, y).finally(() => activeMosaicRequests.delete(key));
  activeMosaicRequests.set(key, request);

  return request;
}

/**
 * Attempts to fetch a cached 256px mosaic tile.
 * If not cached, generates it from a 512px mosaic.
 */
async function cachedMosaic256px(z, x, y) {
  let tileBuffer = await cacheGetTile("__mosaic256__", z, x, y, "png");
  if (tileBuffer) {
    return new Tile(new TileImage(tileBuffer, "png"), z, x, y);
  }

  tileBuffer = await cacheGetTile("__mosaic256__", z, x, y, "jpg");
  if (tileBuffer) {
    return new Tile(new TileImage(tileBuffer, "jpg"), z, x, y);
  }

  const mosaicTile = await mosaic256px(z, x, y);
  await cachePutTile(mosaicTile.image.buffer, "__mosaic256__", z, x, y, mosaicTile.image.extension);
  return mosaicTile;
}

/**
 * Attempts to fetch a cached 512px mosaic tile.
 * If not cached, generates it by calling mosaic512px.
 */
async function cachedMosaic512px(z, x, y) {
  let tileBuffer = await cacheGetTile("__mosaic__", z, x, y, "png");
  if (tileBuffer) {
    return new Tile(new TileImage(tileBuffer, "png"), z, x, y);
  }

  tileBuffer = await cacheGetTile("__mosaic__", z, x, y, "jpg");
  if (tileBuffer) {
    return new Tile(new TileImage(tileBuffer, "jpg"), z, x, y);
  }

  const mosaicTile = await mosaic512px(z, x, y);
  await cachePutTile(mosaicTile.image.buffer, "__mosaic__", z, x, y, mosaicTile.image.extension);
  return mosaicTile;
}

/**
 * Generates a 256px mosaic tile from a 512px tile.
 * If z is even, we scale down a 512px tile.
 * If z is odd, we extract a quarter of a parent 512px tile.
 */
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

/**
 * Generates a 512px mosaic tile:
 * 1. Queries DB for images intersecting at the given z, x, y.
 * 2. Fetches metadata for each image's UUID.
 * 3. If z < 9, also generates a parent tile constructed from children tiles (downsampled).
 * 4. Combines all candidate tiles, sorts them by criteria, and blends them into a final mosaic.
 */
async function mosaic512px(z, x, y, filters = {}) {
  const request512pxFn = Object.keys(filters).length > 0 ? mosaic512px : requestCachedMosaic512px;

  let dbClient;
  let rows;

  // Build a parametrized SQL query to find all images covering this tile.
  const { sqlQuery, sqlQueryParams, queryTag } = buildParametrizedFiltersQuery(
    OAM_LAYER_ID,
    z,
    x,
    y,
    filters
  );

  // Execute the DB query
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

  // Pre-fetch metadata for all rows
  const metadataByUuid = {};
  await Promise.all(
    rows.map(async (row) => {
      if (row?.uuid) {
        metadataByUuid[row.uuid] = await getGeotiffMetadata(row.uuid);
      }
    })
  );

  const rowTiles = [];
  let parentTilePromise = null;

  // If z < 9, we also consider a parent tile made from children tiles at z+1.
  // We first gather tile promises for rows, then handle parent tile separately.
  if (z < 9) {
    for (const row of rows) {
      const meta = metadataByUuid[row.uuid];
      // Only consider images that can contribute at low zoom levels
      if (meta && meta.maxzoom < 9) {
        const key = keyFromS3Url(row.uuid);
        const geojson = JSON.parse(row.geojson);
        rowTiles.push({ row, promise: source(key, z, x, y, meta, geojson) });
      }
    }

    // Parent tile at higher zoom: constructed from 4 sub-tiles (z+1)
    const children = await Promise.all([
      request512pxFn(z + 1, x * 2, y * 2, filters),
      request512pxFn(z + 1, x * 2 + 1, y * 2, filters),
      request512pxFn(z + 1, x * 2, y * 2 + 1, filters),
      request512pxFn(z + 1, x * 2 + 1, y * 2 + 1, filters),
    ]);
    parentTilePromise = constructParentTileFromChildren(children, z, x, y);
  } else {
    // For z >= 9, we just request tiles directly from the source images listed in rows
    for (const row of rows) {
      const meta = metadataByUuid[row.uuid];
      if (!meta) continue;
      const key = keyFromS3Url(row.uuid);
      const geojson = JSON.parse(row.geojson);
      rowTiles.push({ row, promise: source(key, z, x, y, meta, geojson) });
    }
  }

  // Resolve all row-based tile promises
  const resolvedRowTiles = await Promise.all(rowTiles.map((rt) => rt.promise));
  let finalTiles = [];

  // Map resolved tiles back to their rows to get correct metadata by index
  resolvedRowTiles.forEach((tile, index) => {
    const { row } = rowTiles[index];
    const meta = metadataByUuid[row.uuid];
    if (!meta) {
      console.warn(`Null metadata found for uuid ${row.uuid}, skipping...`);
      return;
    }
    if (!tile.empty()) {
      finalTiles.push({ tile, meta });
    }
  });

  // If we have a parent tile, we can add it to finalTiles.
  // It doesn't directly correspond to a row, so we assign neutral metadata
  // or use a placeholder. Adjust as needed for your criteria.
  if (parentTilePromise) {
    const parentTile = await parentTilePromise;
    finalTiles.push({
      tile: parentTile,
      meta: {
        // Using placeholder metadata so it can be included in sorting
        uploaded_at: new Date(0),
        file_size: 0,
        gsd: Infinity,
      },
    });
  }

  // Sort tiles by:
  // 1. Most recent uploaded_at
  // 2. Larger file_size
  // 3. Lower GSD (higher resolution)
  finalTiles.sort((a, b) => {
    const dateA = new Date(a.meta.uploaded_at);
    const dateB = new Date(b.meta.uploaded_at);
    const fileSizeA = a.meta.file_size;
    const fileSizeB = b.meta.file_size;
    const gsdA = a.meta.gsd;
    const gsdB = b.meta.gsd;

    if (dateA.getTime() !== dateB.getTime()) return dateB - dateA;
    if (fileSizeA !== fileSizeB) return fileSizeB - fileSizeA;
    return gsdA - gsdB;
  });

  // Blend all final candidate tiles together
  const tileBuffers = finalTiles.map(({ tile }) => tile.image.buffer);
  const tileBuffer = await blendTiles(tileBuffers, 512, 512);
  const tile = new Tile(new TileImage(tileBuffer, "png"), z, x, y);
  await tile.image.transformInJpegIfFullyOpaque();

  return tile;
}

export { mosaic256px, requestCachedMosaic512px, requestCachedMosaic256px };
