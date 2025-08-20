import { strict as assert } from "node:assert";
import * as db from "./db.mjs";
import {
  cacheGet,
  cachePut,
  cacheDelete,
  mosaicTilesIterable,
  metadataJsonsIterable,
  singleImageTilesIterable,
} from "./cache.mjs";
import { getGeotiffMetadata } from "./metadata.mjs";
import { getTileCover } from "./tile_cover.mjs";
// cacheDeleteBothExtensions is not required because present keys already include extension
import { logger } from "./logging.mjs";

function geojsonGeometryFromBounds(topLeft, bottomRight) {
  return {
    type: "Polygon",
    coordinates: [
      [
        [topLeft[0], topLeft[1]],
        [bottomRight[0], topLeft[1]],
        [bottomRight[0], bottomRight[1]],
        [topLeft[0], bottomRight[1]],
        [topLeft[0], topLeft[1]],
      ],
    ],
  };
}

async function invalidateImage(geojson, maxzoom, presentMosaicCacheKeys) {
  for (let zoom = 0; zoom <= maxzoom; ++zoom) {
    for (const [x, y, z] of getTileCover(geojson, zoom)) {
      const staleCacheKeys = [];
      staleCacheKeys.push(`__mosaic__/${z}/${x}/${y}.png`);
      staleCacheKeys.push(`__mosaic__/${z}/${x}/${y}.jpg`);
      staleCacheKeys.push(`__mosaic256__/${z + 1}/${x * 2}/${y * 2}.png`);
      staleCacheKeys.push(`__mosaic256__/${z + 1}/${x * 2 + 1}/${y * 2}.png`);
      staleCacheKeys.push(`__mosaic256__/${z + 1}/${x * 2}/${y * 2 + 1}.png`);
      staleCacheKeys.push(`__mosaic256__/${z + 1}/${x * 2 + 1}/${y * 2 + 1}.png`);
      staleCacheKeys.push(`__mosaic256__/${z + 1}/${x * 2}/${y * 2}.jpg`);
      staleCacheKeys.push(`__mosaic256__/${z + 1}/${x * 2 + 1}/${y * 2}.jpg`);
      staleCacheKeys.push(`__mosaic256__/${z + 1}/${x * 2}/${y * 2 + 1}.jpg`);
      staleCacheKeys.push(`__mosaic256__/${z + 1}/${x * 2 + 1}/${y * 2 + 1}.jpg`);

      for (const cacheKey of staleCacheKeys) {
        await cacheDelete(cacheKey);
      }
    }
  }
}

function parseMosaicCacheKey(cacheKey) {
  // Formats:
  // __mosaic__/z/x/y.png|jpg
  // __mosaic256__/z/x/y.png|jpg
  const match = cacheKey.match(/^(__mosaic__|__mosaic256__)\/(\d+)\/(\d+)\/(\d+)\.(png|jpg)$/);
  if (!match) return null;
  const [, directory, zStr, xStr, yStr, extension] = match;
  return {
    directory,
    z: Number(zStr),
    x: Number(xStr),
    y: Number(yStr),
    extension,
  };
}

function computeGeojsonBbox(geojson) {
  let minLon = Infinity,
    minLat = Infinity,
    maxLon = -Infinity,
    maxLat = -Infinity;
  const coords = geojson.coordinates[0];
  for (const [lon, lat] of coords) {
    if (lon < minLon) minLon = lon;
    if (lat < minLat) minLat = lat;
    if (lon > maxLon) maxLon = lon;
    if (lat > maxLat) maxLat = lat;
  }
  return [minLon, minLat, maxLon, maxLat];
}

function tileBboxLonLat(z, x, y) {
  const n = Math.pow(2, z);
  const lon1 = (x / n) * 360 - 180;
  const lon2 = ((x + 1) / n) * 360 - 180;
  const latRad1 = Math.atan(Math.sinh(Math.PI * (1 - (2 * y) / n)));
  const latRad2 = Math.atan(Math.sinh(Math.PI * (1 - (2 * (y + 1)) / n)));
  const lat1 = (latRad1 * 180) / Math.PI;
  const lat2 = (latRad2 * 180) / Math.PI;
  const minLon = Math.min(lon1, lon2);
  const maxLon = Math.max(lon1, lon2);
  const minLat = Math.min(lat1, lat2);
  const maxLat = Math.max(lat1, lat2);
  return [minLon, minLat, maxLon, maxLat];
}

function bboxesOverlap(a, b) {
  const [aminLon, aminLat, amaxLon, amaxLat] = a;
  const [bminLon, bminLat, bmaxLon, bmaxLat] = b;
  return !(amaxLon < bminLon || bmaxLon < aminLon || amaxLat < bminLat || bmaxLat < aminLat);
}

async function invalidateImageUsingPresentKeys(geojson, presentMosaicCacheKeys) {
  const geoBbox = computeGeojsonBbox(geojson);
  let invalidatedCount = 0;
  for (const key of presentMosaicCacheKeys) {
    const parsed = parseMosaicCacheKey(key);
    if (!parsed) continue;
    const tileBbox = tileBboxLonLat(parsed.z, parsed.x, parsed.y);
    if (bboxesOverlap(geoBbox, tileBbox)) {
      await cacheDelete(key);
      invalidatedCount += 1;
    }
  }
  return invalidatedCount;
}

const OAM_LAYER_ID = process.env.OAM_LAYER_ID || "openaerialmap";
// How many present mosaic keys to accumulate before processing a batch.
// Can be tuned via env MOSAIC_INVALIDATION_BATCH_SIZE; default 10000.
const MAX_SET_SIZE = Number.parseInt(process.env.MOSAIC_INVALIDATION_BATCH_SIZE, 10) || 2_000;

async function invalidateMosaicCache() {
  const cacheInfo = JSON.parse((await cacheGet("__info__.json")).toString());
  const lastUpdated = new Date(cacheInfo.last_updated);

  logger.debug("Mosaic cache invalidation started");

  let dbClient;
  // uuids and geojsons for newly added images
  let imagesAddedSinceLastInvalidation;
  // uuids of all images currently present in a database. will be used
  // later to compare with the list of images currently present in the cache
  // to identify deleted.
  let allImages;
  try {
    dbClient = await db.getClient();
    let dbResponse = await dbClient.query({
      name: "get-images-added-since-last-invalidation",
      text: `select
          properties->>'uuid' uuid,
          properties->>'uploaded_at' uploaded_at,
          ST_AsGeoJSON(geom) geojson
        from public.layers_features
        where layer_id = (select id from public.layers where public_id = '${OAM_LAYER_ID}')
          and (properties->>'uploaded_at')::timestamp > $1`,
      values: [lastUpdated],
    });
    imagesAddedSinceLastInvalidation = dbResponse.rows;

    logger.debug(
      `Found ${imagesAddedSinceLastInvalidation.length} images since last invalidation at ${lastUpdated}`
    );

    // TODO: it should be possible to join this query with the one above
    dbResponse = await dbClient.query({
      name: "get-all-image-ids",
      text: `select properties->>'uuid' uuid from public.layers_features
        where layer_id = (select id from public.layers where public_id = '${OAM_LAYER_ID}')`,
    });
    allImages = dbResponse.rows;
  } catch (err) {
    throw err;
  } finally {
    if (dbClient) {
      dbClient.release();
    }
  }

  logger.debug(`All ${allImages.length} images count`);

  // First, process deleted images in a single streaming pass over metadata JSONs
  await processDeletedImagesOnce();

  // Compute run-level watermark for added images and track if any were invalidated this run
  let runLatestUploadedAt = null;
  if (imagesAddedSinceLastInvalidation.length > 0) {
    for (const row of imagesAddedSinceLastInvalidation) {
      if (
        !runLatestUploadedAt ||
        Date.parse(row.uploaded_at) > Date.parse(runLatestUploadedAt)
      ) {
        runLatestUploadedAt = row.uploaded_at;
      }
    }
  }
  let runHasInvalidatedAdded = false;

  // Read the list of present mosaic cache keys in batches and process added images incrementally.
  // This avoids reading the entire filesystem tree before doing any useful work.
  const presentMosaicCacheKeys = new Set();
  let scanned = 0;
  for await (const key of mosaicTilesIterable()) {
    presentMosaicCacheKeys.add(key);
    scanned += 1;

    if (presentMosaicCacheKeys.size >= MAX_SET_SIZE) {
      logger.debug(`Invalidation progress: collected ${scanned} present keys, processing batch`);
      await processAddedForBatch(presentMosaicCacheKeys);
      presentMosaicCacheKeys.clear();
    }
  }

  // call processBatch for the last batch
  if (presentMosaicCacheKeys.size > 0) {
    logger.debug(`Invalidation progress: final batch with ${presentMosaicCacheKeys.size} keys`);
    await processAddedForBatch(presentMosaicCacheKeys);
  }

  async function processAddedForBatch(mosaicCacheKeys) {
    if (imagesAddedSinceLastInvalidation.length > 0) {
      for (const row of imagesAddedSinceLastInvalidation) {
        const url = row.uuid;
        const geojson = JSON.parse(row.geojson);

        // Metadata fetching may fail (e.g., TiTiler 404/500) and return null.
        // In that case, skip this image to avoid crashing the whole invalidation job.
        try {
          const meta = await getGeotiffMetadata(url);
          if (meta && typeof meta.maxzoom === "number") {
            await invalidateImage(geojson, meta.maxzoom, mosaicCacheKeys);
            runHasInvalidatedAdded = true;
          } else {
            logger.warn(
              `Metadata missing for ${url}. Falling back to present-keys invalidation.`
            );
            const count = await invalidateImageUsingPresentKeys(geojson, mosaicCacheKeys);
            if (count > 0) runHasInvalidatedAdded = true;
          }
        } catch (error) {
          logger.error(
            `Failed to fetch metadata for ${url}. Falling back to present-keys invalidation.`,
            error
          );
          const count = await invalidateImageUsingPresentKeys(geojson, mosaicCacheKeys);
          if (count > 0) runHasInvalidatedAdded = true;
        }
      }
    }
  }

  async function processDeletedImagesOnce() {
    for await (const metadataCacheKey of metadataJsonsIterable()) {
      const key = metadataCacheKey.replace("__metadata__/", "").replace(".json", "");
      const image = allImages.find((image) => image.uuid.includes(key));
      // if metadata for an image is present in the cache but missing in the origin database
      // all cached mosaic tiles that contain this image need to be invalidated
      if (!image) {
        try {
          const metadataBuffer = await cacheGet(metadataCacheKey);
          if (!metadataBuffer || !metadataBuffer.length) continue;
          const metadata = JSON.parse(metadataBuffer.toString());
          if (!metadata) continue;
          const bounds = metadata.bounds;
          const maxzoom = metadata.maxzoom;
          const geojson = geojsonGeometryFromBounds(bounds.slice(0, 2), bounds.slice(2));
          await invalidateImage(geojson, maxzoom);
          await cacheDelete(metadataCacheKey);
        } catch (error) {
          logger.warn(`metadata cache invalid for key ${metadataCacheKey}`);
          continue;
        }
      }
    }
  }

  if (runHasInvalidatedAdded && runLatestUploadedAt) {
    await cachePut(
      Buffer.from(
        JSON.stringify({
          last_updated: runLatestUploadedAt,
        })
      ),
      "__info__.json"
    );
  }

  logger.debug("Mosaic cache invalidation ended");
}

export { invalidateMosaicCache };
