import sharp from "sharp";
import * as db from "./db.mjs";
import { cacheGet, cachePut, cacheDelete, cachePurgeMosaic } from "./cache.mjs";
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
      if (row?.uuid) {
        metadataByUuid[row.uuid] = await getGeotiffMetadata(row.uuid);
      }
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

  // Begin cloudless stitching logic
  const filteredTiles = tiles
    .filter((tile) => !tile.empty())
    .map((tile, index) => ({
      tile,
      meta: metadataByUuid[rows[index].uuid],
    }));

  // Sort tiles based on the criteria
  filteredTiles.sort((a, b) => {
    // 1. Prefer the latest image
    const dateA = new Date(a.meta.uploaded_at);
    const dateB = new Date(b.meta.uploaded_at);

    // 2. Prefer larger file size
    const fileSizeA = a.meta.file_size;
    const fileSizeB = b.meta.file_size;

    // 3. Prefer higher resolution (lower GSD)
    const gsdA = a.meta.gsd;
    const gsdB = b.meta.gsd;

    // Comparison based on criteria
    if (dateA !== dateB) return dateB - dateA;
    if (fileSizeA !== fileSizeB) return fileSizeB - fileSizeA;
    return gsdA - gsdB;
  });

  const tileBuffers = filteredTiles.map(({ tile }) => tile.image.buffer);

  const tileBuffer = await blendTiles(tileBuffers, 512, 512);

  const tile = new Tile(new TileImage(tileBuffer, "png"), z, x, y);
  await tile.image.transformInJpegIfFullyOpaque();

  return tile;
}

export { mosaic256px, requestCachedMosaic512px, requestCachedMosaic256px };
