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

async function invalidateMosaicCache() {
  const cacheInfo = JSON.parse(await cacheGet("__info__.json"));
  const lastUpdated = new Date(cacheInfo.last_updated);

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
        where layer_id = (select id from public.layers where public_id = 'openaerialmap')
          and (properties->>'uploaded_at')::timestamp > $1`,
      values: [lastUpdated],
    });
    imagesAddedSinceLastInvalidation = dbResponse.rows;

    // TODO: it should be possible to join this query with the one above
    dbResponse = await dbClient.query({
      name: "get-all-image-ids",
      text: `select properties->>'uuid' uuid from public.layers_features
        where layer_id = (select id from public.layers where public_id = 'openaerialmap')`,
    });
    allImages = dbResponse.rows;
  } catch (err) {
    throw err;
  } finally {
    if (dbClient) {
      dbClient.release();
    }
  }

  // read the list of images currently present in the cache so that it can
  // be used later to delete only the tiles that are actually present in
  // the cache
  const presentMosaicCacheKeys = new Set();
  for await (const key of mosaicTilesIterable()) {
    presentMosaicCacheKeys.add(key);
  }

  for await (const metadataCacheKey of metadataJsonsIterable()) {
    const key = metadataCacheKey.replace("__metadata__/", "").replace(".json", "");
    const image = allImages.find((image) => image.uuid.includes(key));
    // if metadata for an image is present in the cache but missing in the "origin" database
    // all cached mosaic tiles that contain this image need to be invalidated
    // because the image itself was deleted.
    if (!image) {
      // rather then calculating tile coverage it is possible to go through
      // all cached tiles for a specific image and mark all mosaic tiles
      // with the same z, x, y as stale
      for await (const singleImageTileCacheKey of singleImageTilesIterable(key)) {
        const [z, x, y] = singleImageTileCacheKey
          .replace(key + "/", "")
          .replace(".png", "") // there are no .jpg tiles for tiles of a single image
          .split("/")
          .map((x) => Number(x));

        const staleCacheKeys = [];
        staleCacheKeys.push(`__mosaic__/${z}/${x}/${y}.png`);
        staleCacheKeys.push(`__mosaic__/${z}/${x}/${y}.jpg`);
        staleCacheKeys.push(`__mosaic256px__/${z + 1}/${x * 2}/${y * 2}.png`);
        staleCacheKeys.push(`__mosaic256px__/${z + 1}/${x * 2 + 1}/${y * 2}.png`);
        staleCacheKeys.push(`__mosaic256px__/${z + 1}/${x * 2}/${y * 2 + 1}.png`);
        staleCacheKeys.push(`__mosaic256px__/${z + 1}/${x * 2 + 1}/${y * 2 + 1}.png`);
        staleCacheKeys.push(`__mosaic256px__/${z + 1}/${x * 2}/${y * 2}.jpg`);
        staleCacheKeys.push(`__mosaic256px__/${z + 1}/${x * 2 + 1}/${y * 2}.jpg`);
        staleCacheKeys.push(`__mosaic256px__/${z + 1}/${x * 2}/${y * 2 + 1}.jpg`);
        staleCacheKeys.push(`__mosaic256px__/${z + 1}/${x * 2 + 1}/${y * 2 + 1}.jpg`);
        for (const key of staleCacheKeys) {
          await cacheDelete(key);
        }
      }

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
      for (let zoom = 0; zoom <= maxzoom; ++zoom) {
        for (const [x, y, z] of getTileCover(geojson, zoom)) {
          const staleCacheKeys = [];
          staleCacheKeys.push(`__mosaic__/${z}/${x}/${y}.png`);
          staleCacheKeys.push(`__mosaic__/${z}/${x}/${y}.jpg`);
          staleCacheKeys.push(`__mosaic256px__/${z + 1}/${x * 2}/${y * 2}.png`);
          staleCacheKeys.push(`__mosaic256px__/${z + 1}/${x * 2 + 1}/${y * 2}.png`);
          staleCacheKeys.push(`__mosaic256px__/${z + 1}/${x * 2}/${y * 2 + 1}.png`);
          staleCacheKeys.push(`__mosaic256px__/${z + 1}/${x * 2 + 1}/${y * 2 + 1}.png`);
          staleCacheKeys.push(`__mosaic256px__/${z + 1}/${x * 2}/${y * 2}.jpg`);
          staleCacheKeys.push(`__mosaic256px__/${z + 1}/${x * 2 + 1}/${y * 2}.jpg`);
          staleCacheKeys.push(`__mosaic256px__/${z + 1}/${x * 2}/${y * 2 + 1}.jpg`);
          staleCacheKeys.push(`__mosaic256px__/${z + 1}/${x * 2 + 1}/${y * 2 + 1}.jpg`);
          for (const key of staleCacheKeys) {
            await cacheDelete(key);
          }
        }
      }
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

export { invalidateMosaicCache };
