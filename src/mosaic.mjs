import sharp from "sharp";
import * as db from "./db.mjs";
import { cacheGet, cachePut, cacheDelete, cachePurgeMosaic } from "./cache.mjs";
import { Tile, TileImage, constructParentTileFromChildren } from "./tile.mjs";
import { enqueueTileFetching } from "./titiler_fetcher.mjs";
import { getGeotiffMetadata } from "./metadata.mjs";
import { getTileCover } from "./tile_cover.mjs";
import { keyFromS3Url } from "./key_from_s3_url.mjs";

const TILE_SIZE = 512;

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

const activeMosaicRequests = new Map();
// wrapper that deduplicates mosiac function calls
function requestMosaic512px(z, x, y) {
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

async function requestMosaic256px(z, x, y) {
  let tileBuffer = await cacheGetTile("__mosaic256px__", z, x, y, "png");
  if (tileBuffer) {
    return new Tile(new TileImage(tileBuffer, "png", 256), z, x, y);
  }

  tileBuffer = await cacheGetTile("__mosaic256px__", z, x, y, "jpg");
  if (tileBuffer) {
    return new Tile(new TileImage(tileBuffer, "jpg", 256), z, x, y);
  }

  let tile256;
  if (z % 2 === 0) {
    const tile512 = await requestMosaic512px(z, x, y);
    tile256 = await tile512.scale(0.5);
  } else {
    const parent512 = await requestMosaic512px(z - 1, x >> 1, y >> 1);
    tile256 = await parent512.extractChild(z, x, y);
  }

  tile256.image.transformInJpegIfFullyOpaque();

  await cachePutTile(
    tile256.image.buffer,
    "__mosaic256px__",
    z,
    x,
    y,
    tile256.image.extension
  );

  return tile256;
}

// request tile for mosaic
async function mosaic(z, x, y) {
  let dbClient;
  let rows;

  let tileBuffer = await cacheGetTile("__mosaic__", z, x, y, "png");
  if (tileBuffer) {
    return new Tile(new TileImage(tileBuffer, "png"), z, x, y);
  }

  tileBuffer = await cacheGetTile("__mosaic__", z, x, y, "jpg");
  if (tileBuffer) {
    return new Tile(new TileImage(tileBuffer, "jpg"), z, x, y);
  }

  try {
    dbClient = await db.getClient();
    const dbResponse = await dbClient.query({
      name: "get-image-uuid-in-zxy-tile",
      text: `with oam_meta as (
          select
              properties->>'gsd' as resolution_in_meters, 
              (properties->>'uploaded_at')::timestamptz as uploaded_at,
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
      constructParentTileFromChildren(
        await Promise.all([
          requestMosaic512px(z + 1, x * 2, y * 2),
          requestMosaic512px(z + 1, x * 2 + 1, y * 2),
          requestMosaic512px(z + 1, x * 2, y * 2 + 1),
          requestMosaic512px(z + 1, x * 2 + 1, y * 2 + 1),
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
          return { input: tile.image.buffer, top: 0, left: 0 };
        })
    )
    .png()
    .toBuffer();

  const tile = new Tile(new TileImage(tileBuffer, "png"), z, x, y);
  await tile.image.transformInJpegIfFullyOpaque();
  await cachePutTile(
    tile.image.buffer,
    "__mosaic__",
    z,
    x,
    y,
    tile.image.extension
  );

  return tile;
}

export { requestMosaic512px, requestMosaic256px };
