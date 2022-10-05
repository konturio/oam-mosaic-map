import { jest } from "@jest/globals";
import fs from "fs";
import EventEmitter from "events";

jest.setTimeout(30000);

jest.unstable_mockModule("../db.mjs", () => ({
  getClient: jest.fn(function () {
    const query = ({ name, values }) => {
      switch (name) {
        case "get-image-uuid-in-zxy-tile": {
          expect(values.length).toBe(3);
          const [z, x, y] = values;
          if (z === 14 && x === 9485 && y === 5610) {
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
        }
        case "get-images-added-since-last-invalidation": {
          expect(values.length).toBe(1);
          expect(values[0]).toBeInstanceOf(Date);
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
        default: {
          throw new Error(
            `unexpected db query with name ${name} and values ${JSON.stringify(
              values
            )}`
          );
        }
      }
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
}

const cache = new CacheMem();

jest.unstable_mockModule("../cache.mjs", () => {
  return {
    cacheGet: cache.get.bind(cache),
    cachePut: cache.put.bind(cache),
    cacheDelete: cache.delete.bind(cache),
  };
});

process.env.TITILER_BASE_URL = "https://test-apps02.konturlabs.com/titiler/";

const {
  requestMosaic,
  tileRequestQueue,
  metadataRequestQueue,
  invalidateMosaicCache,
} = await import("../mosaic.mjs");

test("mosaic(14, 9485, 5610)", async () => {
  const tile = await requestMosaic(14, 9485, 5610);
  expect(tileRequestQueue.size).toBe(0);
  expect(metadataRequestQueue.size).toBe(0);

  const expected = fs.readFileSync("./__tests__/mosaic-14-9485-5610.png");
  expect(Buffer.compare(expected, tile.buffer)).toBe(0);
});

test("mosaic(11, 1233, 637)", async () => {
  const tile = await requestMosaic(11, 1233, 637);
  expect(tileRequestQueue.size).toBe(0);
  expect(metadataRequestQueue.size).toBe(0);

  const expected = fs.readFileSync("./__tests__/mosaic-11-1233-637.png");
  expect(Buffer.compare(expected, tile.buffer)).toBe(0);
});

test("mosaic cache invalidation", async () => {
  const invalidatedCacheKeys = new Set();
  const cacheDeleteEventListener = (key) => {
    invalidatedCacheKeys.add(key);
  };

  const infoBefore = { last_updated: new Date().toISOString() };
  await cache.put(Buffer.from(JSON.stringify(infoBefore)), "__info__.json");

  cache.on("delete", cacheDeleteEventListener);
  await invalidateMosaicCache();
  cache.removeListener("delete", cacheDeleteEventListener);

  const infoAfter = JSON.parse(await cache.get("__info__.json"));

  expect(Date.parse(infoBefore.last_updated)).toBeLessThanOrEqual(
    Date.parse(infoAfter.last_updated)
  );

  expect(invalidatedCacheKeys.has("__mosaic__/0/0/0.png")).toBe(true);
  expect(invalidatedCacheKeys.has("__mosaic__/0/0/0.jpg")).toBe(true);
  expect(invalidatedCacheKeys.has("__mosaic__/11/1233/637.png")).toBe(true);
  expect(invalidatedCacheKeys.has("__mosaic__/11/1233/637.jpg")).toBe(true);
  expect(invalidatedCacheKeys.has("__mosaic__/11/1233/638.png")).toBe(false);
});
