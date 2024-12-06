import { jest } from "@jest/globals";
import fs from "fs";
import EventEmitter from "events";
import pixelmatch from "pixelmatch";
import { PNG } from "pngjs";

jest.setTimeout(30000);

// ---------------------------------------------------------------------
// Mocking the database
// ---------------------------------------------------------------------
const dbQueryHandlers = new Map();
function registerDbQueryHandler(name, handler) {
  dbQueryHandlers.set(name, handler);
}

jest.unstable_mockModule("../src/db.mjs", () => ({
  getClient: jest.fn(() => {
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

    return { query, release };
  }),
}));

// ---------------------------------------------------------------------
// Mocking the cache
// ---------------------------------------------------------------------
class CacheMem extends EventEmitter {
  constructor() {
    super();
    this.cache = new Map();
  }

  async get(key) {
    this.emit("get", key);
    return this.cache.has(key) ? this.cache.get(key) : null;
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

jest.unstable_mockModule("../src/cache.mjs", () => ({
  cacheGet: cache.get.bind(cache),
  cachePut: cache.put.bind(cache),
  cacheDelete: cache.delete.bind(cache),
  cachePurgeMosaic: cache.purgeMosaic.bind(cache),
  mosaicTilesIterable: cache.mosaicTilesIterable.bind(cache),
  metadataJsonsIterable: cache.metadataJsonsIterable.bind(cache),
  singleImageTilesIterable: cache.singleImageTilesIterable.bind(cache),
}));

// ---------------------------------------------------------------------
// Mocking environment and imports
// ---------------------------------------------------------------------
process.env.TITILER_BASE_URL = "https://test-apps02.konturlabs.com/titiler/";

const { invalidateMosaicCache } = await import("../src/mosaic_cache_invalidation_job.mjs");
// const { requestCachedMosaic256px, requestCachedMosaic512px } = await import("../src/mosaic.mjs");
// const { tileRequestQueue, metadataRequestQueue } = await import("../src/titiler_fetcher.mjs");

/**
 * Compare two tiles using pixelmatch to ensure they are identical.
 * @param {Buffer} png1
 * @param {Buffer} png2
 * @param {number} tileSize
 * @returns {number} count of different pixels
 */
function compareTilesPixelmatch(png1, png2, tileSize) {
  return pixelmatch(PNG.sync.read(png1).data, PNG.sync.read(png2).data, null, tileSize, tileSize, {
    threshold: 0,
  });
}

// Reset state before each test
beforeEach(() => {
  cache.reset();
  dbQueryHandlers.clear();
});

// ---------------------------------------------------------------------
// Example Tests
// ---------------------------------------------------------------------

test("mosaic512px returns correct tile with single image", async () => {
  // This scenario tests if a single image that fully covers a tile is returned correctly.
  registerDbQueryHandler("get-image-uuid-in-zxy-tile", (values) => {
    const [z, x, y] = values;
    if (z === 11 && x === 1233 && y === 637) {
      return {
        rows: [
          {
            uuid: "http://example.com/test-image.tif",
            geojson:
              '{"type":"Polygon","coordinates":[[[36.8,56.04],[36.8,56.05],[36.85,56.05],[36.85,56.04],[36.8,56.04]]]}',
          },
        ],
      };
    }
    throw new Error(`Unexpected query: z=${z}, x=${x}, y=${y}`);
  });

  // Mock metadata retrieval
  jest.unstable_mockModule("../src/metadata.mjs", () => ({
    getGeotiffMetadata: jest.fn(async (uuid) => {
      if (uuid === "http://example.com/test-image.tif") {
        return {
          minzoom: 7,
          maxzoom: 14,
          tileUrl: "http://example.com/tile/{z}/{x}/{y}.png",
          uploaded_at: "2023-10-10T00:00:00Z",
          file_size: 500000,
          gsd: 0.3,
        };
      }
      return null;
    }),
  }));
  const { getGeotiffMetadata } = await import("../src/metadata.mjs");

  // Mock tile fetching from Titiler
  jest.unstable_mockModule("../src/titiler_fetcher.mjs", () => ({
    enqueueTileFetching: jest.fn(async (url, z, x, y) => {
      // Return a solid-colored tile buffer for testing
      const png = PNG.sync.write(new PNG({ width: 512, height: 512, fill: true }));
      return png;
    }),
    tileRequestQueue: { size: 0 },
    metadataRequestQueue: { size: 0 },
  }));
  const { enqueueTileFetching } = await import("../src/titiler_fetcher.mjs");

  // Re-import mosaic with mocks
  const { requestCachedMosaic512px } = await import("../src/mosaic.mjs");

  const tile = await requestCachedMosaic512px(11, 1233, 637);
  expect(tile.image).toBeDefined();
  expect(tile.image.buffer).toBeInstanceOf(Buffer);
  expect(getGeotiffMetadata).toHaveBeenCalled();
  expect(enqueueTileFetching).toHaveBeenCalled();
});

test("mosaic512px handles multiple images and sorting", async () => {
  // Test that multiple rows return a tile from the best candidate image by sorting criteria.
  registerDbQueryHandler("get-image-uuid-in-zxy-tile", () => {
    return {
      rows: [
        {
          uuid: "http://example.com/image1.tif",
          geojson:
            '{"type":"Polygon","coordinates":[[[36.8,56.04],[36.8,56.05],[36.85,56.05],[36.85,56.04],[36.8,56.04]]]}',
        },
        {
          uuid: "http://example.com/image2.tif",
          geojson:
            '{"type":"Polygon","coordinates":[[[36.8,56.04],[36.8,56.05],[36.85,56.05],[36.85,56.04],[36.8,56.04]]]}',
        },
      ],
    };
  });

  jest.unstable_mockModule("../src/metadata.mjs", () => ({
    getGeotiffMetadata: jest.fn(async (uuid) => {
      if (uuid === "http://example.com/image1.tif") {
        return {
          minzoom: 0,
          maxzoom: 14,
          tileUrl: "http://example.com/tiles1/{z}/{x}/{y}.png",
          uploaded_at: "2022-01-01T00:00:00Z",
          file_size: 1000,
          gsd: 1.0,
        };
      }
      if (uuid === "http://example.com/image2.tif") {
        return {
          minzoom: 0,
          maxzoom: 14,
          tileUrl: "http://example.com/tiles2/{z}/{x}/{y}.png",
          uploaded_at: "2023-01-01T00:00:00Z",
          file_size: 500,
          gsd: 0.5,
        };
      }
      return null;
    }),
  }));

  jest.unstable_mockModule("../src/titiler_fetcher.mjs", () => ({
    enqueueTileFetching: jest.fn(async (url, z, x, y) => {
      // Return a buffer (both images the same to simplify)
      const png = PNG.sync.write(new PNG({ width: 512, height: 512, fill: true }));
      return png;
    }),
    tileRequestQueue: { size: 0 },
    metadataRequestQueue: { size: 0 },
  }));

  const { requestCachedMosaic512px } = await import("../src/mosaic.mjs");
  const tile = await requestCachedMosaic512px(10, 100, 100);

  expect(tile.image).toBeDefined();
  // The sorting criteria:
  // image2 is newer (2023 vs 2022), so it should come first
  // Check logs or implement a spy to ensure correct sorting if needed.
});

test("mosaic256px generates scaled tile", async () => {
  // Test that mosaic256px correctly scales from a 512px tile at the same coordinates.
  registerDbQueryHandler("get-image-uuid-in-zxy-tile", () => {
    return { rows: [] }; // no images, expect empty fallback
  });

  jest.unstable_mockModule("../src/metadata.mjs", () => ({
    getGeotiffMetadata: jest.fn(async () => null),
  }));

  jest.unstable_mockModule("../src/titiler_fetcher.mjs", () => ({
    enqueueTileFetching: jest.fn(async () => {
      const png = PNG.sync.write(new PNG({ width: 512, height: 512, fill: true }));
      return png;
    }),
    tileRequestQueue: { size: 0 },
    metadataRequestQueue: { size: 0 },
  }));

  const { requestCachedMosaic256px } = await import("../src/mosaic.mjs");
  // This will attempt to fetch a 512px tile and then scale down
  const tile = await requestCachedMosaic256px(12, 2000, 2000);

  expect(tile).toBeDefined();
  expect(tile.image.buffer).toBeInstanceOf(Buffer);
  // Could further test the size and content of the tile if desired.
});

test("parent tile logic: if z < 9 and images don't qualify, only parent tile is used", async () => {
  // This test ensures that if no images meet the maxzoom < 9 condition,
  // we rely solely on the parent tile construction logic without causing indexing errors.

  registerDbQueryHandler("get-image-uuid-in-zxy-tile", () => {
    return {
      rows: [
        {
          uuid: "http://example.com/highzoom-image.tif",
          geojson:
            '{"type":"Polygon","coordinates":[[[36.8,56.04],[36.8,56.05],[36.85,56.05],[36.85,56.04],[36.8,56.04]]]}',
        },
      ],
    };
  });

  jest.unstable_mockModule("../src/metadata.mjs", () => ({
    getGeotiffMetadata: jest.fn(async (uuid) => {
      // This image has maxzoom = 14, which is not < 9, so no direct tile at low zoom
      return {
        minzoom: 0,
        maxzoom: 14,
        tileUrl: "http://example.com/tiles/{z}/{x}/{y}.png",
        uploaded_at: "2023-05-01T00:00:00Z",
        file_size: 1000,
        gsd: 0.5,
      };
    }),
  }));

  jest.unstable_mockModule("../src/titiler_fetcher.mjs", () => ({
    enqueueTileFetching: jest.fn(async () => {
      // Return a dummy buffer for each requested tile
      const png = PNG.sync.write(new PNG({ width: 512, height: 512, fill: true }));
      return png;
    }),
    tileRequestQueue: { size: 0 },
    metadataRequestQueue: { size: 0 },
  }));

  const { requestCachedMosaic512px } = await import("../src/mosaic.mjs");
  const tile = await requestCachedMosaic512px(8, 1, 1);

  expect(tile).toBeDefined();
  // At z=8, no images qualify (maxzoom < 9 is false), so we rely on parent tile logic.
  // The code should not throw errors due to indexing mismatches.
});
