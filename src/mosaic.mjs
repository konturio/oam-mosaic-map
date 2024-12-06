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

// ------------------------------------------------------------------------------------
// Utility functions for cache access
// ------------------------------------------------------------------------------------

/**
 * Get a tile buffer from the cache.
 * @param {string} key - Identifier for the image or mosaic.
 * @param {number} z - Zoom level.
 * @param {number} x - X tile coordinate.
 * @param {number} y - Y tile coordinate.
 * @param {"png"|"jpg"} extension - Image format extension.
 * @returns {Promise<Buffer|null>} The tile buffer, or null if not in cache.
 */
function cacheGetTile(key, z, x, y, extension) {
  if (extension !== "png" && extension !== "jpg") {
    throw new Error(".png and .jpg are the only allowed extensions");
  }
  return cacheGet(`${key}/${z}/${x}/${y}.${extension}`);
}

/**
 * Put a tile buffer into the cache.
 * @param {Buffer|null} tileBuffer - The tile buffer, or null to indicate empty.
 * @param {string} key - Identifier for the image or mosaic.
 * @param {number} z - Zoom level.
 * @param {number} x - X tile coordinate.
 * @param {number} y - Y tile coordinate.
 * @param {"png"|"jpg"} extension - Image format extension.
 * @returns {Promise<void>}
 */
function cachePutTile(tileBuffer, key, z, x, y, extension) {
  if (extension !== "png" && extension !== "jpg") {
    throw new Error(".png and .jpg are the only allowed extensions");
  }
  return cachePut(tileBuffer, `${key}/${z}/${x}/${y}.${extension}`);
}

// ------------------------------------------------------------------------------------
// Tile fetching and assembly logic
// ------------------------------------------------------------------------------------

/**
 * Fetch or construct a source tile for a given image (identified by S3 key and metadata).
 * If zoom is too high or tile doesn't intersect with the image footprint, returns an empty tile.
 * Uses caching to avoid redundant fetches.
 * @param {string} key - The S3 key for the image.
 * @param {number} z - Zoom level.
 * @param {number} x - X tile coordinate.
 * @param {number} y - Y tile coordinate.
 * @param {object} meta - Metadata object with minzoom, maxzoom, and tileUrl.
 * @param {object} geojson - GeoJSON footprint of the image.
 * @returns {Promise<Tile>}
 */
async function source(key, z, x, y, meta, geojson) {
  if (z > meta.maxzoom) {
    return Tile.createEmpty(z, x, y);
  }

  // Try cache first
  let tileBuffer = await cacheGetTile(key, z, x, y, "png");
  if (tileBuffer) {
    return new Tile(new TileImage(tileBuffer, "png"), z, x, y);
  }

  // Check if tile intersects with image footprint
  const tileCover = getTileCover(geojson, z);
  const intersects = tileCover.some((pos) => pos[0] === x && pos[1] === y && pos[2] === z);
  if (!intersects) {
    // Cache null for faster subsequent checks
    await cachePutTile(null, key, z, x, y, "png");
    return Tile.createEmpty(z, x, y);
  }

  // If tile is within the image's zoom range, fetch directly.
  if (z >= meta.minzoom && z <= meta.maxzoom) {
    tileBuffer = await enqueueTileFetching(meta.tileUrl, z, x, y);
  } else if (z < meta.maxzoom) {
    // If the zoom is below maxzoom but not directly available,
    // construct it from children tiles at the next zoom level.
    const children = await Promise.all([
      source(key, z + 1, x * 2, y * 2, meta, geojson),
      source(key, z + 1, x * 2 + 1, y * 2, meta, geojson),
      source(key, z + 1, x * 2, y * 2 + 1, meta, geojson),
      source(key, z + 1, x * 2 + 1, y * 2 + 1, meta, geojson),
    ]);

    const parentTile = await constructParentTileFromChildren(children, z, x, y);
    tileBuffer = parentTile.image.buffer;
  } else {
    // If the tile can't be constructed or fetched, return empty.
    return Tile.createEmpty(z, x, y);
  }

  // Cache the constructed tile
  await cachePutTile(tileBuffer, key, z, x, y, "png");
  return new Tile(new TileImage(tileBuffer, "png"), z, x, y);
}

// ------------------------------------------------------------------------------------
// Mosaic requests with caching and deduplication
// ------------------------------------------------------------------------------------

/**
 * Fetches a cached 256px mosaic tile. If not found in cache, generates it.
 * @param {number} z
 * @param {number} x
 * @param {number} y
 * @returns {Promise<Tile>}
 */
async function cachedMosaic256px(z, x, y) {
  let tileBuffer = await cacheGetTile("__mosaic256__", z, x, y, "png");
  if (tileBuffer) return new Tile(new TileImage(tileBuffer, "png"), z, x, y);

  tileBuffer = await cacheGetTile("__mosaic256__", z, x, y, "jpg");
  if (tileBuffer) return new Tile(new TileImage(tileBuffer, "jpg"), z, x, y);

  const mosaicTile = await mosaic256px(z, x, y);
  await cachePutTile(mosaicTile.image.buffer, "__mosaic256__", z, x, y, mosaicTile.image.extension);
  return mosaicTile;
}

/**
 * Fetches a cached 512px mosaic tile. If not found, calls mosaic512px to generate it.
 * @param {number} z
 * @param {number} x
 * @param {number} y
 * @returns {Promise<Tile>}
 */
async function cachedMosaic512px(z, x, y) {
  let tileBuffer = await cacheGetTile("__mosaic__", z, x, y, "png");
  if (tileBuffer) return new Tile(new TileImage(tileBuffer, "png"), z, x, y);

  tileBuffer = await cacheGetTile("__mosaic__", z, x, y, "jpg");
  if (tileBuffer) return new Tile(new TileImage(tileBuffer, "jpg"), z, x, y);

  const mosaicTile = await mosaic512px(z, x, y);
  await cachePutTile(mosaicTile.image.buffer, "__mosaic__", z, x, y, mosaicTile.image.extension);
  return mosaicTile;
}

// Deduplication map to avoid concurrent duplicate requests
const activeMosaicRequests = new Map();

/**
 * Request a 512px mosaic tile, ensuring no duplicate concurrent requests are performed.
 * @param {number} z
 * @param {number} x
 * @param {number} y
 * @returns {Promise<Tile>}
 */
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
 * Request a 256px mosaic tile (no deduplication needed as it's cheaper).
 * @param {number} z
 * @param {number} x
 * @param {number} y
 * @returns {Promise<Tile>}
 */
function requestCachedMosaic256px(z, x, y) {
  return cachedMosaic256px(z, x, y);
}

// ------------------------------------------------------------------------------------
// Mosaic generation logic
// ------------------------------------------------------------------------------------

/**
 * Generate a 256px mosaic tile:
 * - If z is even, we scale down a 512px tile.
 * - If z is odd, we extract a quarter from a parent 512px tile.
 * - Transform to JPEG if fully opaque.
 * @param {number} z
 * @param {number} x
 * @param {number} y
 * @param {object} filters
 * @returns {Promise<Tile>}
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
 * Generate a 512px mosaic tile:
 * 1. Query database for images covering the specified tile.
 * 2. Fetch their metadata.
 * 3. If z < 9, build a parent tile from next zoom level tiles.
 * 4. Combine candidate tiles, sort, and blend them.
 * @param {number} z
 * @param {number} x
 * @param {number} y
 * @param {object} filters
 * @returns {Promise<Tile>}
 */
async function mosaic512px(z, x, y, filters = {}) {
  const { rows, metadataByUuid } = await fetchRowsAndMetadata(z, x, y, filters);

  // Build the list of tile promises from the rows
  const { rowTiles, parentTilePromise } = buildRowTilePromises(
    z,
    x,
    y,
    rows,
    metadataByUuid,
    filters
  );

  // Resolve all tile promises
  const resolvedRowTiles = await Promise.all(rowTiles.map((rt) => rt.promise));

  // Build final tile list with associated metadata
  let finalTiles = mapTilesWithMetadata(rowTiles, resolvedRowTiles, metadataByUuid);

  // If we have a parent tile, wait for it and add it.
  if (parentTilePromise) {
    const parentTile = await parentTilePromise;
    // Choose meta strategy for parent tile: here we give it neutral "weak" meta
    // so it doesn't dominate actual image tiles.
    finalTiles.push({
      tile: parentTile,
      meta: {
        uploaded_at: new Date(0), // very old date
        file_size: 0,
        gsd: Infinity,
      },
    });
  }

  // Sort tiles by date, file size, and GSD
  sortTiles(finalTiles);

  // Blend all candidate tiles into a single mosaic
  const tile = await blendFinalTiles(finalTiles, z, x, y);
  return tile;
}

// ------------------------------------------------------------------------------------
// Helper functions for mosaic512px
// ------------------------------------------------------------------------------------

/**
 * Fetch rows from the DB and their metadata for the given tile coordinates and filters.
 * @param {number} z
 * @param {number} x
 * @param {number} y
 * @param {object} filters
 * @returns {Promise<{rows: object[], metadataByUuid: object}>}
 */
async function fetchRowsAndMetadata(z, x, y, filters) {
  const { sqlQuery, sqlQueryParams, queryTag } = buildParametrizedFiltersQuery(
    OAM_LAYER_ID,
    z,
    x,
    y,
    filters
  );

  let dbClient;
  let rows;

  try {
    dbClient = await db.getClient();
    const dbResponse = await dbClient.query({
      name: "get-image-uuid-in-zxy-tile" + queryTag,
      text: sqlQuery,
      values: sqlQueryParams,
    });
    rows = dbResponse.rows;
  } catch (error) {
    console.error("Error querying DB:", error);
    throw error;
  } finally {
    if (dbClient) dbClient.release();
  }

  const metadataByUuid = {};
  await Promise.all(
    rows.map(async (row) => {
      if (row?.uuid) {
        metadataByUuid[row.uuid] = await getGeotiffMetadata(row.uuid);
      }
    })
  );

  return { rows, metadataByUuid };
}

/**
 * Build promises for fetching tiles from rows. If z < 9, also build a parent tile promise.
 * @param {number} z
 * @param {number} x
 * @param {number} y
 * @param {object[]} rows
 * @param {object} metadataByUuid
 * @param {object} filters
 * @returns {{rowTiles: {row: object, promise: Promise<Tile>}[], parentTilePromise: Promise<Tile>|null}}
 */
function buildRowTilePromises(z, x, y, rows, metadataByUuid, filters) {
  const rowTiles = [];
  let parentTilePromise = null;
  const request512pxFn = Object.keys(filters).length > 0 ? mosaic512px : requestCachedMosaic512px;

  if (z < 9) {
    // For low zoom levels, consider only images that can contribute at low zoom.
    for (const row of rows) {
      const meta = metadataByUuid[row.uuid];
      if (meta && meta.maxzoom < 9) {
        const key = keyFromS3Url(row.uuid);
        const geojson = JSON.parse(row.geojson);
        rowTiles.push({ row, promise: source(key, z, x, y, meta, geojson) });
      }
    }

    // Build a parent tile from 4 children tiles at the next zoom level
    parentTilePromise = (async () => {
      const children = await Promise.all([
        request512pxFn(z + 1, x * 2, y * 2, filters),
        request512pxFn(z + 1, x * 2 + 1, y * 2, filters),
        request512pxFn(z + 1, x * 2, y * 2 + 1, filters),
        request512pxFn(z + 1, x * 2 + 1, y * 2 + 1, filters),
      ]);
      return constructParentTileFromChildren(children, z, x, y);
    })();
  } else {
    // For higher zoom levels, we directly fetch tiles for all rows
    for (const row of rows) {
      const meta = metadataByUuid[row.uuid];
      if (!meta) continue;
      const key = keyFromS3Url(row.uuid);
      const geojson = JSON.parse(row.geojson);
      rowTiles.push({ row, promise: source(key, z, x, y, meta, geojson) });
    }
  }

  return { rowTiles, parentTilePromise };
}

/**
 * Map the resolved tiles back to their corresponding rows and metadata.
 * Filters out empty tiles and rows without metadata.
 * @param {{row: object, promise: Promise<Tile>}[]} rowTiles
 * @param {Tile[]} resolvedRowTiles
 * @param {object} metadataByUuid
 * @returns {{tile: Tile, meta: object}[]}
 */
function mapTilesWithMetadata(rowTiles, resolvedRowTiles, metadataByUuid) {
  const finalTiles = [];
  resolvedRowTiles.forEach((tile, index) => {
    const { row } = rowTiles[index];
    const meta = metadataByUuid[row.uuid];
    if (!meta) {
      console.warn(`Null metadata found for uuid ${row.uuid}, skipping this tile.`);
      return;
    }
    if (!tile.empty()) {
      finalTiles.push({ tile, meta });
    }
  });
  return finalTiles;
}

/**
 * Sort tiles by:
 * 1. Uploaded date (newest first)
 * 2. File size (larger first)
 * 3. GSD (smaller is better resolution)
 * @param {{tile: Tile, meta: {uploaded_at: string|Date, file_size: number, gsd: number}}[]} finalTiles
 */
function sortTiles(finalTiles) {
  finalTiles.sort((a, b) => {
    const dateA = new Date(a.meta.uploaded_at).getTime();
    const dateB = new Date(b.meta.uploaded_at).getTime();
    const fileSizeA = a.meta.file_size;
    const fileSizeB = b.meta.file_size;
    const gsdA = a.meta.gsd;
    const gsdB = b.meta.gsd;

    if (dateA !== dateB) return dateB - dateA; // newer first
    if (fileSizeA !== fileSizeB) return fileSizeB - fileSizeA; // larger first
    return gsdA - gsdB; // smaller GSD first
  });
}

/**
 * Blend a set of final candidate tiles into one 512x512 tile.
 * Converts to JPEG if fully opaque.
 * @param {{tile: Tile}[]} finalTiles
 * @param {number} z
 * @param {number} x
 * @param {number} y
 * @returns {Promise<Tile>}
 */
async function blendFinalTiles(finalTiles, z, x, y) {
  const tileBuffers = finalTiles.map(({ tile }) => tile.image.buffer);
  const tileBuffer = await blendTiles(tileBuffers, 512, 512);
  const tile = new Tile(new TileImage(tileBuffer, "png"), z, x, y);
  await tile.image.transformInJpegIfFullyOpaque();
  return tile;
}

// ------------------------------------------------------------------------------------
// Exports
// ------------------------------------------------------------------------------------

export { mosaic256px, requestCachedMosaic512px, requestCachedMosaic256px };
