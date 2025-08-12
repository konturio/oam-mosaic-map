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

const OAM_LAYER_ID = process.env.OAM_LAYER_ID || "openaerialmap";
const MAX_SET_SIZE = 1_000_000;

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

  // read the list of images currently present in the cache so that it can
  // be used later to delete only the tiles that are actually present in
  // the cache
  const presentMosaicCacheKeys = new Set();
  for await (const key of mosaicTilesIterable()) {
    if (presentMosaicCacheKeys.size >= MAX_SET_SIZE) {
      // call processBatch when the set reaches the maximum allowed Set size
      await processBatch(presentMosaicCacheKeys);
      presentMosaicCacheKeys.clear();
    }

    presentMosaicCacheKeys.add(key);
  }

  // call processBatch for the last batch
  if (presentMosaicCacheKeys.size > 0) await processBatch(presentMosaicCacheKeys);

  async function processBatch(mosaicCacheKeys) {
    for await (const metadataCacheKey of metadataJsonsIterable()) {
      const key = metadataCacheKey.replace("__metadata__/", "").replace(".json", "");
      const image = allImages.find((image) => image.uuid.includes(key));
      // if metadata for an image is present in the cache but missing in the "origin" database
      // all cached mosaic tiles that contain this image need to be invalidated
      // because the image itself was deleted.
      if (!image) {
        let bounds, maxzoom;
        try {
          const metadataBuffer = await cacheGet(metadataCacheKey);
          if (!metadataBuffer || !metadataBuffer.length) continue;
          const metadata = JSON.parse(metadataBuffer.toString());
          if (!metadata) continue;
          bounds = metadata.bounds;
          maxzoom = metadata.maxzoom;
        } catch (error) {
          logger.warn(`metadata cache invalid for key ${metadataCacheKey}`);
          continue; // skip invalid metadata jsons
        }
        const geojson = geojsonGeometryFromBounds(bounds.slice(0, 2), bounds.slice(2));
        await invalidateImage(geojson, maxzoom, mosaicCacheKeys);
        await cacheDelete(metadataCacheKey);
      }
    }

    let latestUploadedAt;
    if (imagesAddedSinceLastInvalidation.length > 0) {
      latestUploadedAt = imagesAddedSinceLastInvalidation[0].uploaded_at;
      for (const row of imagesAddedSinceLastInvalidation) {
        const url = row.uuid;
        const geojson = JSON.parse(row.geojson);
        if (Date.parse(row.uploaded_at) > Date.parse(latestUploadedAt)) {
          latestUploadedAt = row.uploaded_at;
        }
        const { maxzoom } = await getGeotiffMetadata(url);
        await invalidateImage(geojson, maxzoom, mosaicCacheKeys);
      }
    }

    if (imagesAddedSinceLastInvalidation.length > 0) {
      await cachePut(
        Buffer.from(
          JSON.stringify({
            last_updated: latestUploadedAt,
          })
        ),
        "__info__.json"
      );
    }
  }

  logger.debug("Mosaic cache invalidation ended");
}

export { invalidateMosaicCache };
