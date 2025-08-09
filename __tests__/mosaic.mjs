import { jest } from "@jest/globals";
import fs from "fs";
import EventEmitter from "events";
import pixelmatch from "pixelmatch";
import { PNG } from "pngjs";

jest.setTimeout(30000);

const dbQueryHandlers = new Map();
function registerDbQueryHandler(name, handler) {
  dbQueryHandlers.set(name, handler);
}

jest.unstable_mockModule("../src/db.mjs", () => ({
  getClient: jest.fn(function () {
    const query = ({ name, values }) => {
      if (dbQueryHandlers.has(name)) {
        const handler = dbQueryHandlers.get(name);
        return handler(values);
      }

      throw new Error("undefined database query with name: " + name);
    };

    const release = () => {
      // do nothing
    };

    return {
      query,
      release,
    };
  }),
}));

class CacheMem extends EventEmitter {
  constructor() {
    super();
    this.cache = new Map();
  }

  async get(key) {
    this.emit("get", key);
    if (this.cache.has(key)) {
      return this.cache.get(key);
    }

    return null;
  }

  async put(buffer, key) {
    this.emit("put", key);
    this.cache.set(key, buffer);
  }

  async delete(key) {
    this.emit("delete", key);
    this.cache.delete(key);
  }

  mosaicTilesIterable() {
    const that = this;
    return {
      async *[Symbol.asyncIterator]() {
        for (const key of that.cache.keys()) {
          if (key.startsWith("__mosaic__") || key.startsWith("__mosaic256__")) {
            yield key;
          }
        }
      },
    };
  }

  metadataJsonsIterable() {
    const that = this;
    return {
      async *[Symbol.asyncIterator]() {
        for (const key of that.cache.keys()) {
          if (key.startsWith("__metadata__")) {
            yield key;
          }
        }
      },
    };
  }

  singleImageTilesIterable(uuid) {
    const that = this;
    return {
      async *[Symbol.asyncIterator]() {
        for (const key of that.cache.keys()) {
          if (key.startsWith(uuid)) {
            yield key;
          }
        }
      },
    };
  }

  reset() {
    this.cache.clear();
  }

  purgeMosaic() {
    for (const key of this.cache.keys()) {
      if (key.startsWith("__mosaic__")) {
        this.cache.delete(key);
      }
    }
  }
}

const cache = new CacheMem();

jest.unstable_mockModule("../src/cache.mjs", () => {
  return {
    cacheGet: cache.get.bind(cache),
    cachePut: cache.put.bind(cache),
    cacheDelete: cache.delete.bind(cache),
    cachePurgeMosaic: cache.purgeMosaic.bind(cache),
    mosaicTilesIterable: cache.mosaicTilesIterable.bind(cache),
    metadataJsonsIterable: cache.metadataJsonsIterable.bind(cache),
    singleImageTilesIterable: cache.singleImageTilesIterable.bind(cache),
  };
});

process.env.TITILER_BASE_URL = "https://test-apps02.konturlabs.com/titiler/";

const { requestCachedMosaic256px, requestCachedMosaic512px } = await import("../src/mosaic.mjs");
const { getGeotiffMetadata } = await import("../src/metadata.mjs");
const { invalidateMosaicCache } = await import("../src/mosaic_cache_invalidation_job.mjs");
const { tileRequestQueue, metadataRequestQueue } = await import("../src/titiler_fetcher.mjs");

function compareTilesPixelmatch(png1, png2, tileSize) {
  return pixelmatch(PNG.sync.read(png1).data, PNG.sync.read(png2).data, null, tileSize, tileSize, {
    threshold: 0,
  });
}

beforeEach(() => {
  cache.reset();
  dbQueryHandlers.clear();
});

test("mosaic(14, 9485, 5610) and 2 parent tiles", async () => {
  registerDbQueryHandler("get-image-uuid-in-zxy-tile", (values) => {
    expect(values.length).toBe(3);
    const [z, x, y] = values;
    if (
      (z === 14 && x === 9485 && y === 5610) ||
      (z === 13 && x === 4742 && y === 2805) ||
      (z === 12 && x === 2371 && y === 1402)
    ) {
      return {
        rows: [
          {
            uuid: "https://oin-hotosm.s3.amazonaws.com/60ec2f0a38de2500058775ec/0/60ec2f0a38de2500058775ed.tif",
            geojson:
              '{"type":"Polygon","coordinates":[[[28.3927,49.233978],[28.3927,49.241721],[28.418338,49.241721],[28.418338,49.233978],[28.3927,49.233978]]]}',
          },
          {
            uuid: "https://oin-hotosm.s3.amazonaws.com/60f93a91bdbb2f00062bcbe9/0/60f93a91bdbb2f00062bcbea.tif",
            geojson:
              '{"type":"Polygon","coordinates":[[[28.424527,49.231163],[28.424527,49.236791],[28.429493,49.236791],[28.429493,49.231163],[28.424527,49.231163]]]}',
          },
        ],
      };
    }

    throw new Error(`query received unexpected values: ${JSON.stringify(values)}`);
  });

  const [tile, parentTile, parentParentTile] = await Promise.all([
    requestCachedMosaic512px(14, 9485, 5610),
    requestCachedMosaic512px(13, 4742, 2805),
    requestCachedMosaic512px(12, 2371, 1402),
  ]);

  if (process.env.UPDATE_SNAPSHOTS) {
    fs.writeFileSync("./__tests__/mosaic@2x-14-9485-5610.png", tile.image.buffer);
    fs.writeFileSync("./__tests__/mosaic@2x-13-4742-2805.png", parentTile.image.buffer);
    fs.writeFileSync("./__tests__/mosaic@2x-12-2371-1402.png", parentParentTile.image.buffer);
  }

  expect(
    compareTilesPixelmatch(
      fs.readFileSync("./__tests__/mosaic@2x-14-9485-5610.png"),
      tile.image.buffer,
      512
    )
  ).toBe(0);
  expect(
    compareTilesPixelmatch(
      fs.readFileSync("./__tests__/mosaic@2x-13-4742-2805.png"),
      parentTile.image.buffer,
      512
    )
  ).toBe(0);
  expect(
    compareTilesPixelmatch(
      fs.readFileSync("./__tests__/mosaic@2x-12-2371-1402.png"),
      parentParentTile.image.buffer,
      512
    )
  ).toBe(0);
});

test("mosaic256px(14, 9485, 5610)", async () => {
  registerDbQueryHandler("get-image-uuid-in-zxy-tile", (values) => {
    expect(values.length).toBe(3);
    const [z, x, y] = values;
    if (
      (z === 14 && x === 9485 && y === 5610) ||
      (z === 13 && x === 4742 && y === 2805) ||
      (z === 12 && x === 2371 && y === 1402)
    ) {
      return {
        rows: [
          {
            uuid: "https://oin-hotosm.s3.amazonaws.com/60ec2f0a38de2500058775ec/0/60ec2f0a38de2500058775ed.tif",
            geojson:
              '{"type":"Polygon","coordinates":[[[28.3927,49.233978],[28.3927,49.241721],[28.418338,49.241721],[28.418338,49.233978],[28.3927,49.233978]]]}',
          },
          {
            uuid: "https://oin-hotosm.s3.amazonaws.com/60f93a91bdbb2f00062bcbe9/0/60f93a91bdbb2f00062bcbea.tif",
            geojson:
              '{"type":"Polygon","coordinates":[[[28.424527,49.231163],[28.424527,49.236791],[28.429493,49.236791],[28.429493,49.231163],[28.424527,49.231163]]]}',
          },
        ],
      };
    }

    throw new Error(`query received unexpected values: ${JSON.stringify(values)}`);
  });

  const tile = await requestCachedMosaic256px(15, 18970, 11220);

  if (process.env.UPDATE_SNAPSHOTS) {
    fs.writeFileSync("./__tests__/mosaic@1x-15-18970-11220.png", tile.image.buffer);
  }

  expect(
    compareTilesPixelmatch(
      fs.readFileSync("./__tests__/mosaic@1x-15-18970-11220.png"),
      tile.image.buffer,
      256
    )
  ).toBe(0);
});

test("mosaic(11, 1233, 637)", async () => {
  registerDbQueryHandler("get-image-uuid-in-zxy-tile", (values) => {
    expect(values.length).toBe(3);
    const [z, x, y] = values;
    if (z === 11 && x === 1233 && y === 637) {
      return {
        rows: [
          {
            uuid: "http://oin-hotosm.s3.amazonaws.com/59b4275223c8440011d7ae10/0/9837967b-4639-4788-a13f-0c5eb8278be1.tif",
            geojson:
              '{"type":"Polygon","coordinates":[[[36.835672447,56.043330146],[36.835672447,56.048091024],[36.847995133,56.048091024],[36.847995133,56.043330146],[36.835672447,56.043330146]]]}',
          },
        ],
      };
    }

    throw new Error(`query received unexpected values: ${JSON.stringify(values)}`);
  });

  const tile = await requestCachedMosaic512px(11, 1233, 637);
  expect(tileRequestQueue.size).toBe(0);
  expect(metadataRequestQueue.size).toBe(0);

  const expected = fs.readFileSync("./__tests__/mosaic@2x-11-1233-637.png");

  if (process.env.UPDATE_SNAPSHOTS) {
    fs.writeFileSync("./__tests__/mosaic@2x-11-1233-637.png", tile.image.buffer);
  }

  expect(compareTilesPixelmatch(expected, tile.image.buffer, 512)).toBe(0);
});

test("mosaic cache invalidation [add]", async () => {
  registerDbQueryHandler("get-images-added-since-last-invalidation", (values) => {
    expect(values.length).toBe(1);
    expect(values[0]).toBeInstanceOf(Date);
    return {
      rows: [
        {
          uuid: "http://oin-hotosm.s3.amazonaws.com/59b4275223c8440011d7ae10/0/9837967b-4639-4788-a13f-0c5eb8278be1.tif",
          uploaded_at: "2022-10-06T03:40:19.040Z",
          geojson:
            '{"type":"Polygon","coordinates":[[[36.835672447,56.043330146],[36.835672447,56.048091024],[36.847995133,56.048091024],[36.847995133,56.043330146],[36.835672447,56.043330146]]]}',
        },
      ],
    };
  });

  registerDbQueryHandler("get-all-image-ids", () => {
    return {
      rows: [
        {
          uuid: "http://oin-hotosm.s3.amazonaws.com/59b4275223c8440011d7ae10/0/9837967b-4639-4788-a13f-0c5eb8278be1.tif",
        },
      ],
    };
  });

  const invalidatedCacheKeys = new Set();
  const cacheDeleteEventListener = (key) => {
    invalidatedCacheKeys.add(key);
  };

  const infoBefore = { last_updated: "2022-10-05T03:40:19.040Z" };
  await cache.put(Buffer.from(JSON.stringify(infoBefore)), "__info__.json");

  await Promise.all([
    cache.put(null, "__mosaic__/0/0/0.png"),
    cache.put(null, "__mosaic__/0/0/0.jpg"),
    cache.put(null, "__mosaic__/11/1233/637.png"),
    cache.put(null, "__mosaic256__/12/2466/1274.png"),
    cache.put(null, "__mosaic__/11/1233/637.jpg"),
    cache.put(null, "__mosaic256__/12/2466/1274.jpg"),
    cache.put(null, "__mosaic__/11/1233/637.jpg"),
    cache.put(null, "__mosaic256__/12/2466/1274.jpg"),
    cache.put(null, "__mosaic__/11/1233/638.png"),
  ]);

  cache.on("delete", cacheDeleteEventListener);
  await invalidateMosaicCache();
  cache.removeListener("delete", cacheDeleteEventListener);

  const infoAfter = JSON.parse(await cache.get("__info__.json"));

  expect(new Date(Date.parse(infoAfter.last_updated)).toISOString()).toBe(infoAfter.last_updated);
  expect(infoAfter.last_updated).toBe("2022-10-06T03:40:19.040Z");
  expect(Date.parse(infoBefore.last_updated)).toBeLessThanOrEqual(
    Date.parse(infoAfter.last_updated)
  );

  expect(invalidatedCacheKeys.has("__mosaic__/0/0/0.png")).toBe(true);
  expect(invalidatedCacheKeys.has("__mosaic__/0/0/0.jpg")).toBe(true);
  expect(invalidatedCacheKeys.has("__mosaic__/11/1233/637.png")).toBe(true);
  expect(invalidatedCacheKeys.has("__mosaic256__/12/2466/1274.png")).toBe(true);
  expect(invalidatedCacheKeys.has("__mosaic__/11/1233/637.jpg")).toBe(true);
  expect(invalidatedCacheKeys.has("__mosaic256__/12/2466/1274.jpg")).toBe(true);
  expect(invalidatedCacheKeys.has("__mosaic__/11/1233/638.png")).toBe(false);
});

test("mosaic cache invalidation [delete]", async () => {
  registerDbQueryHandler("get-image-uuid-in-zxy-tile", (values) => {
    expect(values.length).toBe(3);
    const [z, x, y] = values;
    if (z === 11 && x === 1233 && y === 637) {
      return {
        rows: [
          {
            uuid: "http://oin-hotosm.s3.amazonaws.com/59b4275223c8440011d7ae10/0/9837967b-4639-4788-a13f-0c5eb8278be1.tif",
            geojson:
              '{"type":"Polygon","coordinates":[[[36.835672447,56.043330146],[36.835672447,56.048091024],[36.847995133,56.048091024],[36.847995133,56.043330146],[36.835672447,56.043330146]]]}',
          },
        ],
      };
    }

    throw new Error(`query received unexpected values: ${JSON.stringify(values)}`);
  });

  await requestCachedMosaic512px(11, 1233, 637);

  registerDbQueryHandler("get-images-added-since-last-invalidation", (values) => {
    expect(values.length).toBe(1);
    expect(values[0]).toBeInstanceOf(Date);
    return {
      rows: [],
    };
  });

  registerDbQueryHandler("get-all-image-ids", () => {
    return {
      rows: [],
    };
  });

  const invalidatedCacheKeys = new Set();
  const cacheDeleteEventListener = (key) => {
    invalidatedCacheKeys.add(key);
  };

  const infoBefore = { last_updated: "2022-10-05T03:40:19.040Z" };
  await cache.put(Buffer.from(JSON.stringify(infoBefore)), "__info__.json");

  await Promise.all([
    cache.put(null, "__mosaic__/0/0/0.png"),
    cache.put(null, "__mosaic__/0/0/0.jpg"),
    cache.put(null, "__mosaic__/11/1233/637.png"),
    cache.put(null, "__mosaic256__/12/2466/1274.png"),
    cache.put(null, "__mosaic__/11/1233/637.jpg"),
    cache.put(null, "__mosaic256__/12/2466/1274.jpg"),
    cache.put(null, "__mosaic__/11/1233/637.jpg"),
    cache.put(null, "__mosaic256__/12/2466/1274.jpg"),
    cache.put(null, "__mosaic__/11/1233/638.png"),
  ]);

  cache.on("delete", cacheDeleteEventListener);
  await invalidateMosaicCache();
  cache.removeListener("delete", cacheDeleteEventListener);

  expect(
    invalidatedCacheKeys.has(
      "__metadata__/59b4275223c8440011d7ae10/0/9837967b-4639-4788-a13f-0c5eb8278be1.json"
    )
  ).toBe(true);
  expect(invalidatedCacheKeys.has("__mosaic__/0/0/0.png")).toBe(true);
  expect(invalidatedCacheKeys.has("__mosaic__/0/0/0.jpg")).toBe(true);
  expect(invalidatedCacheKeys.has("__mosaic__/11/1233/637.png")).toBe(true);
  expect(invalidatedCacheKeys.has("__mosaic256__/12/2466/1274.png")).toBe(true);
  expect(invalidatedCacheKeys.has("__mosaic__/11/1233/637.jpg")).toBe(true);
  expect(invalidatedCacheKeys.has("__mosaic256__/12/2466/1274.jpg")).toBe(true);
  expect(invalidatedCacheKeys.has("__mosaic__/11/1233/638.png")).toBe(false);
});
