import * as db from "./db.mjs";
import {
  cacheGet,
  cachePut,
  cacheDelete,
  mosaicTilesIterable,
} from "./cache.mjs";
import { getGeotiffMetadata } from "./metadata.mjs";
import { getTileCover } from "./tile_cover.mjs";

async function invalidateMosaicCache() {
  const cacheInfo = JSON.parse(await cacheGet("__info__.json"));
  const lastUpdated = new Date(cacheInfo.last_updated);

  let dbClient;
  let rows;
  try {
    dbClient = await db.getClient();
    const dbResponse = await dbClient.query({
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
    rows = dbResponse.rows;
  } catch (err) {
    throw err;
  } finally {
    if (dbClient) {
      dbClient.release();
    }
  }

  if (rows.length === 0) {
    return;
  }

  const staleCacheKeys = new Set();
  let latestUploadedAt = rows[0].uploaded_at;
  for (const row of rows) {
    const url = row.uuid;
    const geojson = JSON.parse(row.geojson);
    if (Date.parse(row.uploaded_at) > Date.parse(latestUploadedAt)) {
      latestUploadedAt = row.uploaded_at;
    }
    const { maxzoom } = await getGeotiffMetadata(url);
    for (let zoom = 0; zoom <= maxzoom; ++zoom) {
      for (const [x, y, z] of getTileCover(geojson, zoom)) {
        staleCacheKeys.add(`__mosaic__/${z}/${x}/${y}.png`);
        staleCacheKeys.add(`__mosaic__/${z}/${x}/${y}.jpg`);

        staleCacheKeys.add(`__mosaic256px__/${z + 1}/${x * 2}/${y * 2}.png`);
        staleCacheKeys.add(`__mosaic256px__/${z + 1}/${x * 2 + 1}/${y * 2}.png`);
        staleCacheKeys.add(`__mosaic256px__/${z + 1}/${x * 2}/${y * 2 + 1}.png`);
        staleCacheKeys.add(`__mosaic256px__/${z + 1}/${x * 2 + 1}/${y * 2 + 1}.png`);

        staleCacheKeys.add(`__mosaic256px__/${z + 1}/${x * 2}/${y * 2}.jpg`);
        staleCacheKeys.add(`__mosaic256px__/${z + 1}/${x * 2 + 1}/${y * 2}.jpg`);
        staleCacheKeys.add(`__mosaic256px__/${z + 1}/${x * 2}/${y * 2 + 1}.jpg`);
        staleCacheKeys.add(`__mosaic256px__/${z + 1}/${x * 2 + 1}/${y * 2 + 1}.jpg`);
      }
    }
  }

  /*
    if tile coverage of area that need to be invalidated is too large
    it should be faster to iterate through all tiles currently present
    in cache and check if they are present in invalidated area coverage
    rather than making thousands or millions of rm calls.
  */
  if (staleCacheKeys.size > 10_000) {
    for await (const key of mosaicTilesIterable()) {
      if (staleCacheKeys.has(key)) {
        await cacheDelete(key);
      }
    }
  } else {
    for (const key of staleCacheKeys.keys()) {
      await cacheDelete(key);
    }
  }

  await cachePut(
    Buffer.from(
      JSON.stringify({
        last_updated: latestUploadedAt,
      })
    ),
    "__info__.json"
  );
}

export { invalidateMosaicCache };
