import sharp from "sharp";
import * as db from "./db.mjs";
import { cacheGet, cachePut } from "./cache.mjs";
import { Tile, TileImage, constructParentTileFromChildren } from "./tile.mjs";
import { enqueueTileFetching } from "./titiler_fetcher.mjs";
import { getGeotiffMetadata } from "./metadata.mjs";
import { getTileCover } from "./tile_cover.mjs";
import { keyFromS3Url } from "./key_from_s3_url.mjs";
import { buildParametrizedFiltersQuery } from "./filters.mjs";
import { blendTiles, blendBackTiles } from "./tileBlender.mjs";

const OAM_LAYER_ID = process.env.OAM_LAYER_ID || "openaerialmap";

const PREFERRED_PALETTE_RGB = [
  [14, 61, 66],
  [219, 210, 95],
  [73, 127, 138],
  [173, 115, 102],
  [140, 140, 140],
  [69, 77, 62],
  [112, 143, 57],
  [174, 170, 166],
];

function tileYToLat(y, z) {
  const n = Math.PI - (2 * Math.PI * y) / Math.pow(2, z);
  return (180 / Math.PI) * Math.atan(0.5 * (Math.exp(n) - Math.exp(-n)));
}

function tilePixelSizeMeters(z, y) {
  const lat = tileYToLat(y + 0.5, z);
  return (
    156543.03392 * Math.cos((lat * Math.PI) / 180) / Math.pow(2, z)
  );
}

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

  const pixelSize = tilePixelSizeMeters(z, y);
  const front = [];
  const back = [];
  for (const t of finalTiles) {
    const gsd = t.meta.gsd ?? Infinity;
    if (gsd <= pixelSize) front.push(t); else back.push(t);
  }

  sortFrontTiles(front, z);
  sortBackTiles(back);

  const backBuffer = back.length
    ? await blendBackTiles(
        back.map((b) => b.tile.image.buffer),
        512,
        512,
        PREFERRED_PALETTE_RGB
      )
    : null;
  const frontBuffer = front.length
    ? await blendTiles(front.map((f) => f.tile.image.buffer), 512, 512)
    : null;

  let finalBuffer;
  if (backBuffer && frontBuffer) {
    finalBuffer = await sharp(backBuffer)
      .composite([{ input: frontBuffer }])
      .png()
      .toBuffer();
  } else if (frontBuffer) {
    finalBuffer = frontBuffer;
  } else if (backBuffer) {
    finalBuffer = backBuffer;
  } else {
    finalBuffer = await sharp({
      create: {
        width: 512,
        height: 512,
        channels: 4,
        background: { r: 0, g: 0, b: 0, alpha: 0 },
      },
    })
      .png()
      .toBuffer();
  }

  const tile = new Tile(new TileImage(finalBuffer, "png"), z, x, y);
  await tile.image.transformInJpegIfFullyOpaque();
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
    rows = dbResponse.rows.map((r) => ({
      uuid: r.uuid,
      geojson: r.geojson,
      resolution_in_meters: r.resolution_in_meters
        ? Number.parseFloat(r.resolution_in_meters)
        : null,
      acquisition_end: r.acquisition_end,
      uploaded_at: r.uploaded_at,
    }));
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
        const m = await getGeotiffMetadata(row.uuid);
        metadataByUuid[row.uuid] = {
          ...m,
          uploaded_at: row.uploaded_at || row.acquisition_end,
          gsd: row.resolution_in_meters,
        };
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
      finalTiles.push({
        tile,
        meta: {
          ...meta,
          file_size: tile.image.buffer ? tile.image.buffer.length : 0,
        },
      });
    }
  });
  return finalTiles;
}

function roundDateByZoom(date, z) {
  const d = new Date(date);
  if (z <= 4) return new Date(0); // ignore date
  if (z <= 8) return new Date(Date.UTC(d.getUTCFullYear(), 0, 1));
  if (z <= 12) return new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), 1));
  return d; // full precision for high zoom
}

function sortFrontTiles(frontTiles, z) {
  frontTiles.sort((a, b) => {
    const dateA = roundDateByZoom(a.meta.uploaded_at, z).getTime();
    const dateB = roundDateByZoom(b.meta.uploaded_at, z).getTime();
    if (dateA !== dateB) return dateB - dateA;
    const fileSizeA = a.meta.file_size;
    const fileSizeB = b.meta.file_size;
    if (fileSizeA !== fileSizeB) return fileSizeB - fileSizeA;
    const gsdA = a.meta.gsd ?? Infinity;
    const gsdB = b.meta.gsd ?? Infinity;
    return gsdA - gsdB;
  });
}

function sortBackTiles(backTiles) {
  backTiles.sort((a, b) => {
    const gsdA = a.meta.gsd ?? Infinity;
    const gsdB = b.meta.gsd ?? Infinity;
    return gsdA - gsdB;
  });
}

// ------------------------------------------------------------------------------------
// Exports
// ------------------------------------------------------------------------------------

export { mosaic256px, requestCachedMosaic512px, requestCachedMosaic256px };
