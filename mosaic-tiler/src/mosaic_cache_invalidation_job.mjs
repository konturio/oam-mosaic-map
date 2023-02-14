import * as db from "./db.mjs";
import { cacheGet, cachePut, cacheDelete, cachePurgeMosaic } from "./cache.mjs";
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

  const invalidTilePaths = [];
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
        invalidTilePaths.push(`__mosaic__/${z}/${x}/${y}.png`);
        invalidTilePaths.push(`__mosaic__/${z}/${x}/${y}.jpg`);
        invalidTilePaths.push(`__mosaic256px__/${z + 1}/${x * 2}/${y * 2}.png`);
        invalidTilePaths.push(`__mosaic256px__/${z + 1}/${x * 2}/${y * 2}.jpg`);
      }
    }
  }
  await Promise.all(invalidTilePaths.map((path) => cacheDelete(path)));

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
